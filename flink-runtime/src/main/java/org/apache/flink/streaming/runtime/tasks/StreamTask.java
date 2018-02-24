/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.PausingOperatorMarker;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.SpillablePipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.RuntimeEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OperatorSnapshotResult;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;
import org.apache.flink.streaming.runtime.modification.ModificationMetaData;
import org.apache.flink.streaming.runtime.modification.events.CancelModificationMarker;
import org.apache.flink.streaming.runtime.modification.statemigration.StateMigrationMetaData;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FutureUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 * <p>
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 * <p>
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 * <p>
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 * <p>
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
	extends AbstractInvokable
	implements StatefulTask, AsyncExceptionHandler {

	/**
	 * The thread group that holds all trigger timer threads.
	 */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/**
	 * The logger used by the StreamTask and its subclasses.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	enum ModificationStatus {
		NOT_MODIFIED,
		SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK, // Sources only
		SPILLED_TO_DISK_AFTER_CHECKPOINT, // Sources only // TODO Really?
		SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_PAUSE_OPERATOR, // Sources only
		OPERATOR_WAITING_FOR_UPCOMING_CHECKPOINT_TO_BROADCAST_NEW_DOWNSTREAM_OPERATOR_LOCATION, // Sources only
		WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR, // Operators only
		UPDATING_INPUT_CHANNEL_IN_REPONSE_TO_MIGRATING_UPSTREAM_OPERATOR, // Operators only
		BROADCASTED_NEW_LOCATION_FOR_DOWNSTREAM_OPERATOR,
		PAUSING,
	}

	private static final AtomicReferenceFieldUpdater<StreamTask, ModificationStatus> STATE_UPDATER =
		AtomicReferenceFieldUpdater.newUpdater(StreamTask.class, ModificationStatus.class, "modificationStatus");

	private volatile ModificationStatus modificationStatus = ModificationStatus.NOT_MODIFIED;

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/**
	 * the head operator that consumes the input streams of this task.
	 */
	protected OP headOperator;

	/**
	 * The chain of operators executed by this task.
	 */
	protected OperatorChain<OUT, OP> operatorChain;

	/**
	 * The configuration of this streaming task.
	 */
	private StreamConfig configuration;

	/**
	 * Our state backend. We use this to create checkpoint streams and a keyed state backend.
	 */
	private StateBackend stateBackend;

	/**
	 * Keyed state backend for the head operator, if it is keyed. There can only ever be one.
	 */
	private AbstractKeyedStateBackend<?> keyedStateBackend;

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	private ProcessingTimeService timerService;

	/**
	 * The map of user-defined accumulators of this task.
	 */
	private Map<String, Accumulator<?, ?>> accumulatorMap;

	private TaskStateHandles restoreStateHandles;

	/**
	 * The currently active background materialization threads.
	 */
	private CloseableRegistry cancelables;

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/**
	 * Flag to mark this task as canceled.
	 */
	private volatile boolean canceled;

	volatile boolean pausedForModification = false;

	/**
	 * Thread pool for async snapshot workers.
	 */
	private ExecutorService asyncOperationsThreadPool;

	private final Map<Long, AtomicLong> seenModificationMarker = new HashMap<>();

	private volatile ModificationCoordinator.ModificationAction modificationAction =
		ModificationCoordinator.ModificationAction.NO_OP;

	private List<InputChannelDeploymentDescriptor> icddToBroadcastDownstream;

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public boolean getPausedForModification() {
		return pausedForModification;
	}

	public void setPausedForModification(boolean pausedForModification) {
		this.pausedForModification = pausedForModification;
	}

	public ModificationCoordinator.ModificationAction getModificationAction() {
		return modificationAction;
	}

	/**
	 * Allows the user to specify his own {@link ProcessingTimeService TimerServiceProvider}.
	 * By default a {@link SystemProcessingTimeService DefaultTimerService} is going to be provided.
	 * Changing it can be useful for testing processing time functionality, such as
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}
	 * and {@link org.apache.flink.streaming.api.windowing.triggers.Trigger Triggers}.
	 */
	public void setProcessingTimeService(ProcessingTimeService timeProvider) {
		if (timeProvider == null) {
			throw new RuntimeException("The timeProvider cannot be set to null.");
		}
		timerService = timeProvider;
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {

			// -------- Initialize ---------
			LOG.info("Initializing {} with migration {}.", getName(), pausedForModification);

			cancelables = new CloseableRegistry();

			configuration = new StreamConfig(getTaskConfiguration());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			stateBackend = createStateBackend();

			accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();

			// if the clock is not already set, then assign a default TimeServiceProvider
			if (timerService == null) {
				ThreadFactory timerThreadFactory =
					new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this);
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.info("Invoking {} with chain {}", getName(), operatorChain);

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				initializeState();
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			if (function != null) {
				LOG.error("BENCHMARKING: Using different user function");
				switchFunction(function);
			}

			// let the task do its work
			pausedForModification = false;
			isRunning = true;
			run();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			synchronized (lock) {

				// Paused this StreamTask for modification, do not attempt to clean up
				if (pausedForModification) {

					LOG.debug("Paused task {} for migration", getName());

					triggerStateMigration();

				} else {
					LOG.debug("Task {} stopped but not for migration", getName());
				}
			}

			// make sure all timers finish and no new timers can come
			timerService.quiesceAndAwaitPending();

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				isRunning = false;

				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();
			}

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		} finally {

			LOG.debug("Cleanup for task {} from modification {}", getName(), pausedForModification);

			// clean up everything we initialized
			isRunning = false;

			// stop all timers and threads
			if (timerService != null) {
				try {
					timerService.shutdownService();
				} catch (Throwable t) {
					// catch and log the exception to not replace the original exception
					LOG.error("Could not shut down timer service", t);
				}
			}

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			LOG.debug("Closing Operators for task {} from modification {}", getName(), pausedForModification);

			// we must! perform this cleanup
			try {
				cleanup();
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				operatorChain.releaseOutputs();
			}
		}
	}

	@Override
	public void pause() throws Exception {
		isRunning = false;
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		} finally {
			cancelables.close();
		}
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	/**
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #openAllOperators()}.
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 * <p>
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				} catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 * <p>
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 *
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 *
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	Output<StreamRecord<OUT>> getHeadOutput() {
		return operatorChain.getChainEntryPoint();
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@Override
	public void setInitialState(TaskStateHandles taskStateHandles) {
		this.restoreStateHandles = taskStateHandles;
	}

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
				.setBytesBufferedInAlignment(0L)
				.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		} catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointMetrics checkpointMetrics) throws Exception {

		try {

			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		} catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
				getName(), checkpointMetaData.getCheckpointId());
			throw e;
		} catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	private void triggerStateMigration() throws Exception {

		LOG.error("BENCHMARK:Operator {} ({}) is now starting state migration", getName(), getEnvironment().getExecutionId());

		StateMigrationMetaData stateMigrationMetaData =
			new StateMigrationMetaData(new Random().nextLong(), System.currentTimeMillis());

		// No alignment if we inject a checkpoint
		CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
			.setBytesBufferedInAlignment(0L)
			.setAlignmentDurationNanos(0L);

		try {
			performStateTransferToJobManager(stateMigrationMetaData, checkpointMetrics);
		} catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing state migration {}.",
				getName(), stateMigrationMetaData.getStateMigrationId());
			throw e;
		} catch (Exception e) {
			throw new Exception("Could not perform state migration " + stateMigrationMetaData.getStateMigrationId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	@Override
	public void abortModification(ModificationMetaData modificationMetaData,
								  Set<ExecutionAttemptID> executionAttemptIDS,
								  Throwable cause) throws Exception {
		LOG.debug("Aborting modification via cancel-barrier {} for task {}", modificationMetaData.getModificationID(), getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineModification(modificationMetaData.getModificationID(), cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCancelModificationEvent(modificationMetaData, executionAttemptIDS);
		}
	}

	@Override
	public void updateChannelLocation(int channelIndex, InputChannelDeploymentDescriptor inputChannelDeploymentDescriptor) {

		synchronized (lock) {
			if (isRunning){
				switch (STATE_UPDATER.get(this)) {

					case WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR:
						break;

					case PAUSING:
						// Happens if we receive a new icdd, although we want to pause
						// Therefore, ignore this message
						break;

					case UPDATING_INPUT_CHANNEL_IN_REPONSE_TO_MIGRATING_UPSTREAM_OPERATOR:
					case NOT_MODIFIED:
					case SPILLED_TO_DISK_AFTER_CHECKPOINT:

						InputGate[] allInputGates = getEnvironment().getAllInputGates();
						SingleInputGate inputGate;

						if (allInputGates.length != 1) {

							// TODO Masterthesis, only works for one and two input streams

							inputGate = mapIndexToSingleInputGate(allInputGates, channelIndex);

							channelIndex = mapIndexToChannelIndex(allInputGates, channelIndex);

							if (inputGate == null || channelIndex == -1) {
								throw new IllegalStateException("Error in logic");
							}
						} else {
							inputGate = (SingleInputGate) allInputGates[0];
						}

						updateInputChannel(inputGate, channelIndex, inputChannelDeploymentDescriptor);

						transitionState(ModificationStatus.NOT_MODIFIED,ModificationStatus.UPDATING_INPUT_CHANNEL_IN_REPONSE_TO_MIGRATING_UPSTREAM_OPERATOR);
						transitionState(ModificationStatus.NOT_MODIFIED,ModificationStatus.SPILLED_TO_DISK_AFTER_CHECKPOINT);

						if (STATE_UPDATER.get(this) != ModificationStatus.UPDATING_INPUT_CHANNEL_IN_REPONSE_TO_MIGRATING_UPSTREAM_OPERATOR
							&& STATE_UPDATER.get(this) != ModificationStatus.SPILLED_TO_DISK_AFTER_CHECKPOINT) {
							throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
						}

						break;

					case SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK:
					case SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_PAUSE_OPERATOR:
					default:
						throw new IllegalStateException(getName() + " - " + STATE_UPDATER.get(this));
				}
			} else {
				throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
			}
		}
	}

	private int mapIndexToChannelIndex(InputGate[] allInputGates, int channelIndex) {
		int index = 0;

		for (InputGate allInputGate : allInputGates) {
			for (int j = 0; j < allInputGate.getNumberOfInputChannels(); j++) {
				if (index == channelIndex) {
					return j;
				}
				index++;
			}
		}

		return -1;
	}

	public SingleInputGate mapIndexToSingleInputGate(InputGate[] allInputGates, int channelIndex) {
		int index = 0;

		for (InputGate allInputGate : allInputGates) {
			for (int j = 0; j < allInputGate.getNumberOfInputChannels(); j++) {
				if (index == channelIndex) {
					return (SingleInputGate) allInputGate;
				}
				index++;
			}
		}

		return null;
	}

	private void updateInputChannel(SingleInputGate inputGate, int channelIndex, InputChannelDeploymentDescriptor icdd) {

		NetworkEnvironment networkEnvironment = ((RuntimeEnvironment) getEnvironment()).getNetwork();

		final ResultPartitionID partitionId = icdd.getConsumedPartitionId();
		final ResultPartitionLocation partitionLocation = icdd.getConsumedPartitionLocation();

		InputChannel inputChannel;

		LOG.info("Modifying InputGate with new {}InputChannel for SubTaskIndex {}",
			partitionLocation.isLocal() ? "Local" : "Remote", channelIndex);

		if (icdd.getConsumedPartitionLocation().isLocal()) {
			inputChannel = new LocalInputChannel(inputGate,
				channelIndex,
				partitionId,
				networkEnvironment.getResultPartitionManager(),
				networkEnvironment.getTaskEventDispatcher(),
				networkEnvironment.getPartitionRequestInitialBackoff(),
				networkEnvironment.getPartitionRequestMaxBackoff(),
				getEnvironment().getMetricGroup().getIOMetricGroup());
		} else {
			inputChannel = new RemoteInputChannel(
				inputGate,
				channelIndex,
				partitionId,
				partitionLocation.getConnectionId(),
				networkEnvironment.getConnectionManager(),
				networkEnvironment.getPartitionRequestInitialBackoff(),
				networkEnvironment.getPartitionRequestMaxBackoff(),
				getEnvironment().getMetricGroup().getIOMetricGroup(),
				getName());
		}

		try {
			inputGate.updateInputChannel(partitionId.getPartitionId(), inputChannel, channelIndex);
		} catch (IOException | InterruptedException e) {
			throw new IllegalStateException("Could not update InputChannel", e);
		}

		LOG.debug("Successfully connected {} to new Input", getName());
	}

	private volatile boolean alreadyTriggerMigration = false;

	private volatile long modificationID;

	@Override
	public boolean triggerMigration(ModificationMetaData metaData,
									Map<ExecutionAttemptID, Set<Integer>> spillingVertices,
									Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices,
									Set<ExecutionAttemptID> notPausingOperators,
									long upcomingCheckpointID) throws Exception {

		try {

			LOG.info("Starting migration ({}) for '{}' on task {} with jobVertexID {}",
				metaData.getModificationID(),
				StringUtils.join(spillingVertices.entrySet(), ","),
				getName(),
				getEnvironment().getJobVertexId());

			synchronized (lock) {
				if (isRunning) {
					// we can do a modification

					if (!alreadyTriggerMigration) {
						alreadyTriggerMigration = true;
					} else {
						return true;
					}

					modificationID = metaData.getModificationID();

					Set<Integer> spillingIndices = spillingVertices.get(getEnvironment().getExecutionId());
					List<InputChannelDeploymentDescriptor> location = stoppingVertices.get(getEnvironment().getExecutionId());

					if (spillingIndices != null) {

						// TODO Masterthesis, check if correct of need to differentiate between sources & operators
						if (transitionState(ModificationStatus.NOT_MODIFIED,
							ModificationStatus.SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK)) {

							setupSpillingToDiskOnUpcomingCheckpoint(metaData,
								spillingIndices,
								upcomingCheckpointID,
								ModificationCoordinator.ModificationAction.STOPPING);

						} else {
							throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
						}

					} else if (location != null) {

						if (!isSinkOperator()) {
							Preconditions.checkArgument(getStreamOutputs().length == 1);
							Preconditions.checkArgument(getStreamOutputs()[0].getRecordWriter().getNumChannels() == location.size());
						}

						if (notPausingOperators.contains(getEnvironment().getExecutionId())) {
							if (!transitionState(ModificationStatus.NOT_MODIFIED,
								ModificationStatus.OPERATOR_WAITING_FOR_UPCOMING_CHECKPOINT_TO_BROADCAST_NEW_DOWNSTREAM_OPERATOR_LOCATION)) {
								throw new IllegalStateException("Unexpected state for {}" + getName());
							}

							this.icddToBroadcastDownstream = location;
							this.checkpointToReactTo = upcomingCheckpointID;

						} else 	if (this instanceof SourceStreamTask &&
							transitionState(ModificationStatus.NOT_MODIFIED,
							ModificationStatus.SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_PAUSE_OPERATOR)) {

							this.icddToBroadcastDownstream = location;
							this.checkpointToReactTo = upcomingCheckpointID;

						} else if (transitionState(ModificationStatus.NOT_MODIFIED,
							ModificationStatus.WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR)) {

							this.icddToBroadcastDownstream = location;
							this.checkpointToReactTo = upcomingCheckpointID;

						} else {
							throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
						}

					} else {
						getEnvironment().ignoreModification(metaData.getModificationID());
						LOG.info("Could not find {} in vertices for {}, that should be modified. Not attempting to pause operator",
							getEnvironment().getJobVertexId(), getName());
					}

					// Since both state checkpointing and downstream barrier emission occurs in this
					// lock scope, they are an atomic operation regardless of the order in which they occur.
					// Given this, we immediately emit the checkpoint barriers, so the downstream operators
					// can start their checkpoint work as soon as possible
					operatorChain.broadcastStartMigrationEvent(metaData, spillingVertices, stoppingVertices, notPausingOperators, upcomingCheckpointID);

					return true;
				} else {
					// we cannot perform our modification - let the downstream operators know that they
					// should not wait for any input from this operator

					// we cannot broadcast the modification markers on the 'operator chain', because it may not
					// yet be created
					final CancelModificationMarker cancelModificationMarker =
						new CancelModificationMarker(metaData.getModificationID(), metaData.getTimestamp(), new HashSet<ExecutionAttemptID>());

					Exception exception = null;

					for (ResultPartitionWriter output : getEnvironment().getAllWriters()) {
						try {
							output.writeBufferToAllChannels(EventSerializer.toBuffer(cancelModificationMarker));
						} catch (Exception e) {
							exception = ExceptionUtils.firstOrSuppressed(
								new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
								exception);
						}
					}

					if (exception != null) {
						throw exception;
					}

					return false;
				}
			}
		} catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform modification " + metaData.getModificationID() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform modification {} for operator {} while the " +
					"invokable was not in state running.", metaData.getModificationID(), getName(), e);
				return false;
			}
		}
	}

	private Set<Integer> spillingIndices;
	private long checkpointToReactTo = -10;

	private void setupSpillingToDiskOnUpcomingCheckpoint(ModificationMetaData metaData,
														 Set<Integer> spillingIndices,
														 long upcomingCheckpointID,
														 ModificationCoordinator.ModificationAction action) {

		LOG.info("Found {} in vertices for {}, that should be modified. Past modifications: {}",
			getEnvironment().getJobVertexId(), getName(), Joiner.on(",")
				.join(getEnvironment().getModificationHandler().getHandledModifications()));

		if (upcomingCheckpointID == -1) {
			for (RecordWriterOutput<?> recordWriterOutput : getStreamOutputs()) {
				for (Integer subTaskIndex : spillingIndices) {

					recordWriterOutput
						.getRecordWriter()
						.getResultPartitionWriter()
						.spillImmediately(subTaskIndex, action);
				}
			}
			LOG.info("Operator {} successfully started sending spilling markers for {}.",
				getName(), getEnvironment().getJobVertexId());
			getEnvironment().acknowledgeModification(metaData.getModificationID());
		} else {
			this.spillingIndices = spillingIndices;
			this.checkpointToReactTo = upcomingCheckpointID;

			LOG.info("Operator {} successfully started registered for sending spilling markers for {}.",
				getName(), getEnvironment().getJobVertexId());
			getEnvironment().acknowledgeModification(metaData.getModificationID());
		}
	}

	protected abstract boolean pauseInputs();

	private boolean checkIfModificationAlreadyOngoing(long modificationID) {

		if (seenModificationMarker.get(modificationID) == null) {
			seenModificationMarker.put(modificationID, new AtomicLong(0));
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean triggerModification(ModificationMetaData metaData,
									   Set<ExecutionAttemptID> executionAttemptIDS,
									   Set<Integer> subTasksToPause,
									   ModificationCoordinator.ModificationAction action) throws Exception {
		return triggerModification(metaData, executionAttemptIDS, subTasksToPause, action, -1);
	}

	@Override
	public boolean triggerModification(ModificationMetaData metaData,
									   Set<ExecutionAttemptID> executionAttemptIDS,
									   Set<Integer> subTasksToPause,
									   ModificationCoordinator.ModificationAction action,
									   long upcomingCheckpointID) throws Exception {
		try {

			LOG.info("Starting modification ({}) for '{}' on task {} with jobVertexID {}",
				metaData.getModificationID(),
				StringUtils.join(executionAttemptIDS, ","),
				getName(),
				getEnvironment().getJobVertexId());

			synchronized (lock) {
				if (isRunning) {
					// we can do a modification

					if (executionAttemptIDS.contains(getEnvironment().getExecutionId())) {
						LOG.info("Found {} in vertices for {}, that should be modified. Past modifications: {}",
							getEnvironment().getJobVertexId(), getName(), Joiner.on(",").join(getEnvironment().getModificationHandler().getHandledModifications()));

						if (checkIfModificationAlreadyOngoing(metaData.getModificationID())) {
							if (transitionState(ModificationStatus.NOT_MODIFIED, ModificationStatus.SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK)) {

								modificationAction = action;

								setupSpillingToDiskOnUpcomingCheckpoint(metaData, subTasksToPause, upcomingCheckpointID, ModificationCoordinator.ModificationAction.STOPPING);

								LOG.info("Operator {} successfully started modification sequence for {}.",
									getName(), getEnvironment().getJobVertexId());
								getEnvironment().acknowledgeModification(metaData.getModificationID());
							} else {
								getEnvironment().declineModification(metaData.getModificationID(),
									new Throwable("Could not transition status for " + getName()));
								throw new IllegalStateException("Could not transitionState. Current status is: " + modificationStatus);
							}
						} else {
							LOG.info("Already received modification marker {} for {} for modification {}.",
								seenModificationMarker.get(metaData.getModificationID()).get(), getName(), metaData.getModificationID());
						}

					} else {
						getEnvironment().ignoreModification(metaData.getModificationID());
						LOG.info("Could not find {} in vertices for {}, that should be modified. Not attempting to pause operator",
							getEnvironment().getJobVertexId(), getName());
					}

					// Since both state checkpointing and downstream barrier emission occurs in this
					// lock scope, they are an atomic operation regardless of the order in which they occur.
					// Given this, we immediately emit the checkpoint barriers, so the downstream operators
					// can start their checkpoint work as soon as possible
					operatorChain.broadcastStartModificationEvent(metaData, executionAttemptIDS, subTasksToPause, action);

					return true;
				} else {
					// we cannot perform our modification - let the downstream operators know that they
					// should not wait for any input from this operator

					// we cannot broadcast the modification markers on the 'operator chain', because it may not
					// yet be created
					final CancelModificationMarker cancelModificationMarker =
						new CancelModificationMarker(metaData.getModificationID(), metaData.getTimestamp(), executionAttemptIDS);

					Exception exception = null;

					for (ResultPartitionWriter output : getEnvironment().getAllWriters()) {
						try {
							output.writeBufferToAllChannels(EventSerializer.toBuffer(cancelModificationMarker));
						} catch (Exception e) {
							exception = ExceptionUtils.firstOrSuppressed(
								new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
								exception);
						}
					}

					if (exception != null) {
						throw exception;
					}

					return false;
				}
			}
		} catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform modification " + metaData.getModificationID() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform modification {} for operator {} while the " +
					"invokable was not in state running.", metaData.getModificationID(), getName(), e);
				return false;
			}
		}
	}

	/**
	 * Only meant for sources, to shut down
	 */
	private void shutdownAfterSendingOperatorPausedEvent(ModificationCoordinator.ModificationAction action) {
		modificationAction = action;

		if (pauseInputs()) {

			if (transitionState(ModificationStatus.SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_PAUSE_OPERATOR, ModificationStatus.PAUSING)) {

				LOG.error("BENCHMARK:Operator {} is now pausing: {}.", getName(), getEnvironment().getExecutionId());
			} else {
				throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
			}

		} else {
			LOG.info("Operator {} failed to pause Input for SpillingToDisk: {}.",
				getName(), getEnvironment().getJobVertexId());
		}
	}

	@Override
	public boolean receivedBlockingMarkersFromAllInputs(ModificationCoordinator.ModificationAction action) throws Exception {


		LOG.info("Operator {} acknowledges SpillingToDisk {}.", getName(), getEnvironment().getJobVertexId());

		synchronized (lock) {
			if (isRunning) {

				ModificationStatus modificationStatus = STATE_UPDATER.get(this);

				if (modificationStatus != ModificationStatus.WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR) {
//			throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
					// Not interested in pausing marker, if we not are pausing for migration
					// Channel update will be handled separately
					return true;
				}

				modificationAction = action;

				broadcastPausingMarkersDownstream();

				if (pauseInputs()) {

					if (transitionState(ModificationStatus.WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR, ModificationStatus.PAUSING)) {

						LOG.error("BENCHMARK:Operator {} is now pausing: {}.", getName(), getEnvironment().getExecutionId());

						return true;
					} else {

						LOG.info("Operator {} successfully acknowledged SpillingToDisk but failed to transition state: {}.",
							getName(), getEnvironment().getJobVertexId());

						return false;
					}

				} else {
					LOG.info("Operator {} failed to pause Input for SpillingToDisk: {}.", getName(), getEnvironment().getJobVertexId());

					return false;
				}
			} else {
				LOG.info("Operator {} wants to acknowledge SpillingToDisk, but is not running: {}.", getName(), getEnvironment().getJobVertexId());

				return false;
			}
		}
	}

	private boolean performCheckpoint(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint

				// Since both state checkpointing and downstream barrier emission occurs in this
				// lock scope, they are an atomic operation regardless of the order in which they occur.
				// Given this, we immediately emit the checkpoint barriers, so the downstream operators
				// can start their checkpoint work as soon as possible
				operatorChain.broadcastCheckpointBarrier(
					checkpointMetaData.getCheckpointId(),
					checkpointMetaData.getTimestamp(),
					checkpointOptions);

				// TODO basically does two checkpoints, the normal one, and the one for sending the state if exiting
				reactOnCheckpoint(checkpointMetaData.getCheckpointId());

				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);

				return true;
			} else {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (ResultPartitionWriter output : getEnvironment().getAllWriters()) {
					try {
						output.writeBufferToAllChannels(EventSerializer.toBuffer(message));
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

				return false;
			}
		}
	}

	private void reactOnCheckpoint(long checkpointId) throws IOException, InterruptedException {

		synchronized (lock) {
			if (isRunning) {
				if (checkpointId == checkpointToReactTo) {

					ResultPartition resultPartition;
					ResultSubpartition[] subpartitions;

					switch (STATE_UPDATER.get(this)) {
						case WAITING_FOR_SPILLING_MARKER_TO_PAUSE_OPERATOR:
						case PAUSING:
						case NOT_MODIFIED:
						case UPDATING_INPUT_CHANNEL_IN_REPONSE_TO_MIGRATING_UPSTREAM_OPERATOR:
							break;

						case OPERATOR_WAITING_FOR_UPCOMING_CHECKPOINT_TO_BROADCAST_NEW_DOWNSTREAM_OPERATOR_LOCATION:

							LOG.error("BENCHMARK:Operator {} ({}) is now broadcasting new location.", getName(), getEnvironment().getExecutionId());

							broadcastPausingMarkersDownstream();

							resultPartition = getStreamOutputs()[0].getRecordWriter().getResultPartitionWriter().getResultPartition();

							subpartitions = resultPartition.getSubpartitions();

							for (ResultSubpartition subpartition : subpartitions) {
								((SpillablePipelinedSubpartition) subpartition).spillToDiskWithoutMarker();
							}

							if (!transitionState(ModificationStatus.OPERATOR_WAITING_FOR_UPCOMING_CHECKPOINT_TO_BROADCAST_NEW_DOWNSTREAM_OPERATOR_LOCATION,
								ModificationStatus.BROADCASTED_NEW_LOCATION_FOR_DOWNSTREAM_OPERATOR)) {
								throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
							}

							getEnvironment().acknowledgeSpillingForNewOperator(modificationID);

							break;

						case SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK:

							LOG.error("BENCHMARK:Operator {} ({}) is now spilling to disk", getName(), getEnvironment().getExecutionId());

							resultPartition = getStreamOutputs()[0].getRecordWriter().getResultPartitionWriter().getResultPartition();

							subpartitions = resultPartition.getSubpartitions();

							for (Integer spillingIndex : spillingIndices) {
								((SpillablePipelinedSubpartition) subpartitions[spillingIndex])
									.spillToDiskWithoutMarker(ModificationCoordinator.ModificationAction.STOPPING);
							}

							if (!transitionState(ModificationStatus.SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_SPILL_TO_DISK,
								ModificationStatus.SPILLED_TO_DISK_AFTER_CHECKPOINT)) {
								throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
							}

							break;

						case SOURCE_WAITING_FOR_UPCOMING_CHECKPOINT_TO_PAUSE_OPERATOR:

							// Pausing operator, and therefore propagating new location

							LOG.error("BENCHMARK:Operator {} ({}) is now pausing and propagating new location.", getName(), getEnvironment().getExecutionId());

							broadcastPausingMarkersDownstream();

							shutdownAfterSendingOperatorPausedEvent(ModificationCoordinator.ModificationAction.STOPPING);

							break;

						default:
					}
				}
			} else {
				throw new IllegalStateException("" + getName() + " - " + STATE_UPDATER.get(this));
			}
		}
	}

	private void broadcastPausingMarkersDownstream() throws IOException, InterruptedException {

		if (isSinkOperator()) {
			return;
		}

		Preconditions.checkArgument(icddToBroadcastDownstream.size()
			== getStreamOutputs()[0].getRecordWriter().getNumChannels());

		for (int i = 0; i < icddToBroadcastDownstream.size(); i++) {
			getStreamOutputs()[0]
				.getRecordWriter()
				.sendEventToTarget(new PausingOperatorMarker(icddToBroadcastDownstream.get(i)), i);
		}
	}

	private boolean isSinkOperator() {
		return getName().toLowerCase().contains("sink");
	}

	private boolean performStateTransferToJobManager(StateMigrationMetaData stateMigrationMetaData,
													 CheckpointMetrics checkpointMetrics) throws Exception {
		LOG.debug("Starting state migration ({}) on task {}",
			stateMigrationMetaData.getStateMigrationId(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a state migration

				checkpointStateForMigration(stateMigrationMetaData, checkpointMetrics);
				return true;
			} else {
				LOG.debug("Failed state migration ({}) on task {} as it is not running",
					stateMigrationMetaData.getStateMigrationId(), getName());

				return false;
			}
		}
	}

	private void checkpointStateForMigration(StateMigrationMetaData stateMigrationMetaData,
											 CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.error("BENCHMARK:Operator {} ({}) is now starting synchronous checkpoint part", getName(), getEnvironment().getExecutionId());

		SynchronousCheckpointingOperation synchronousCheckpointingOperation = new SynchronousCheckpointingOperation(
			this,
			stateMigrationMetaData,
			CheckpointOptions.forFullCheckpoint(),
			checkpointMetrics);

		synchronousCheckpointingOperation.executeCheckpointing();
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyOfCompletedCheckpoint(checkpointId);
					}
				}
			} else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	@Override
	public void switchFunction(Function newUserFunction) {
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		assert allOperators.length == 1;

		StreamFilter streamFilter = (StreamFilter) allOperators[0];

		LOG.error("Saw operator {}", streamFilter);

		streamFilter.setFilterFunction((FilterFunction) newUserFunction);
	}

	private void checkpointState(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			checkpointMetrics);

		checkpointingOperation.executeCheckpointing();
	}

	private void initializeState() throws Exception {

		boolean restored = null != restoreStateHandles;

		if (restored) {
			checkRestorePreconditions(operatorChain.getChainLength());
			initializeOperators(true);
			restoreStateHandles = null; // free for GC
		} else {
			initializeOperators(false);
		}
	}

	private void initializeOperators(boolean restored) throws Exception {
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int chainIdx = 0; chainIdx < allOperators.length; ++chainIdx) {
			StreamOperator<?> operator = allOperators[chainIdx];
			if (null != operator) {
				if (restored && restoreStateHandles != null) {
					operator.initializeState(new OperatorStateHandles(restoreStateHandles, chainIdx));
				} else {
					operator.initializeState(null);
				}
			}
		}
	}

	private void checkRestorePreconditions(int operatorChainLength) {

		ChainedStateHandle<StreamStateHandle> nonPartitionableOperatorStates =
			restoreStateHandles.getLegacyOperatorState();
		List<Collection<OperatorStateHandle>> operatorStates =
			restoreStateHandles.getManagedOperatorState();

		if (nonPartitionableOperatorStates != null) {
			Preconditions.checkState(nonPartitionableOperatorStates.getLength() == operatorChainLength,
				"Invalid Invalid number of operator states. Found :" + nonPartitionableOperatorStates.getLength()
					+ ". Expected: " + operatorChainLength);
		}

		if (!CollectionUtil.isNullOrEmpty(operatorStates)) {
			Preconditions.checkArgument(operatorStates.size() == operatorChainLength,
				"Invalid number of operator states. Found :" + operatorStates.size() +
					". Expected: " + operatorChainLength);
		}
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState     of the execution
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ModificationStatus currentState, ModificationStatus newState) {
		return transitionState(currentState, newState, null);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState     of the execution
	 * @param cause        of the transition change or null
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ModificationStatus currentState, ModificationStatus newState, Throwable cause) {

		if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
			if (cause == null) {
				LOG.info("{} ({}) switched from {} to {}.", getName(), getEnvironment().getExecutionId(), currentState, newState);
			} else {
				LOG.info("{} ({}) switched from {} to {} with exception {}.", getName(), getEnvironment().getExecutionId(), currentState, newState, cause);
			}

			return true;
		} else {
			return false;
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromJob = configuration.getStateBackend(getUserCodeClassLoader());

		if (fromJob != null) {
			// backend has been configured on the environment
			LOG.info("Using user-defined state backend: {}.", fromJob);
			return fromJob;
		} else {
			return AbstractStateBackend.loadStateBackendFromConfigOrCreateDefault(
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
		}
	}

	public OperatorStateBackend createOperatorStateBackend(
		StreamOperator<?> op, Collection<OperatorStateHandle> restoreStateHandles) throws Exception {

		Environment env = getEnvironment();
		String opId = createOperatorIdentifier(op, getConfiguration().getVertexID());

		OperatorStateBackend operatorStateBackend = stateBackend.createOperatorStateBackend(env, opId);

		// let operator state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(operatorStateBackend);

		// restore if we have some old state
		if (null != restoreStateHandles) {
			operatorStateBackend.restore(restoreStateHandles);
		}

		return operatorStateBackend;
	}

	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange) throws Exception {

		if (keyedStateBackend != null) {
			throw new RuntimeException("The keyed state backend can only be created once.");
		}

		String operatorIdentifier = createOperatorIdentifier(
			headOperator,
			configuration.getVertexID());

		keyedStateBackend = stateBackend.createKeyedStateBackend(
			getEnvironment(),
			getEnvironment().getJobID(),
			operatorIdentifier,
			keySerializer,
			numberOfKeyGroups,
			keyGroupRange,
			getEnvironment().getTaskKvStateRegistry());

		// let keyed state backend participate in the operator lifecycle, i.e. make it responsive to cancelation
		cancelables.registerClosable(keyedStateBackend);

		// restore if we have some old state
		Collection<KeyedStateHandle> restoreKeyedStateHandles =
			restoreStateHandles == null ? null : restoreStateHandles.getManagedKeyedState();

		keyedStateBackend.restore(restoreKeyedStateHandles);

		@SuppressWarnings("unchecked")
		AbstractKeyedStateBackend<K> typedBackend = (AbstractKeyedStateBackend<K>) keyedStateBackend;
		return typedBackend;
	}

	/**
	 * This is only visible because
	 * {@link org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink} uses the
	 * checkpoint stream factory to write write-ahead logs. <b>This should not be used for
	 * anything else.</b>
	 */
	public CheckpointStreamFactory createCheckpointStreamFactory(StreamOperator<?> operator) throws IOException {
		return stateBackend.createStreamFactory(
			getEnvironment().getJobID(),
			createOperatorIdentifier(operator, configuration.getVertexID()));
	}

	public CheckpointStreamFactory createSavepointStreamFactory(StreamOperator<?> operator, String targetLocation) throws IOException {
		return stateBackend.createSavepointStreamFactory(
			getEnvironment().getJobID(),
			createOperatorIdentifier(operator, configuration.getVertexID()),
			targetLocation);
	}

	private String createOperatorIdentifier(StreamOperator<?> operator, int vertexId) {
		return operator.getClass().getSimpleName() +
			"_" + vertexId +
			"_" + getEnvironment().getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	public ProcessingTimeService getProcessingTimeService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 * <p>
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		if (isRunning) {
			// only fail if the task is still running
			getEnvironment().failExternally(exception);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	private static final class AsyncCheckpointRunnable implements Runnable, Closeable {

		private static final String ASYNC_DURATION_MILLIS = "asyncDurationMillis";
		public static final String SYNC_DURATION_MILLIS = "syncDurationMillis";
		public static final String ALIGNMENT_DURATION_MILLIS = "alignmentDurationMillis";
		public static final String BYTES_ALIGNMENT = "bytesAlignment";

		private final StreamTask<?, ?> owner;

		private final List<OperatorSnapshotResult> snapshotInProgressList;

		private RunnableFuture<KeyedStateHandle> futureKeyedBackendStateHandles;
		private RunnableFuture<KeyedStateHandle> futureKeyedStreamStateHandles;

		private List<StreamStateHandle> nonPartitionedStateHandles;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;

		private final AtomicReference<CheckpointingOperation.AsynCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsynCheckpointState.RUNNING);

		AsyncCheckpointRunnable(
			StreamTask<?, ?> owner,
			List<StreamStateHandle> nonPartitionedStateHandles,
			List<OperatorSnapshotResult> snapshotInProgressList,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.snapshotInProgressList = Preconditions.checkNotNull(snapshotInProgressList);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.nonPartitionedStateHandles = nonPartitionedStateHandles;
			this.asyncStartNanos = asyncStartNanos;

			if (!snapshotInProgressList.isEmpty()) {
				// TODO Currently only the head operator of a chain can have keyed state, so simply access it directly.
				int headIndex = snapshotInProgressList.size() - 1;
				OperatorSnapshotResult snapshotInProgress = snapshotInProgressList.get(headIndex);
				if (null != snapshotInProgress) {
					this.futureKeyedBackendStateHandles = snapshotInProgress.getKeyedStateManagedFuture();
					this.futureKeyedStreamStateHandles = snapshotInProgress.getKeyedStateRawFuture();
				}
			}

			//Ventura: motification starts
			TaskMetricGroup tmGroup = owner.getEnvironment().getMetricGroup();
			tmGroup.gauge(ASYNC_DURATION_MILLIS, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return 0L;
				}
			});

			tmGroup.gauge(SYNC_DURATION_MILLIS, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return 0L;
				}
			});


			tmGroup.gauge(ALIGNMENT_DURATION_MILLIS, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return 0L;
				}
			});


			tmGroup.gauge(BYTES_ALIGNMENT, new Gauge<Long>() {
				@Override
				public Long getValue() {
					return 0L;
				}
			});
		}

		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {
				// Keyed state handle future, currently only one (the head) operator can have this
				KeyedStateHandle keyedStateHandleBackend = FutureUtil.runIfNotDoneAndGet(futureKeyedBackendStateHandles);
				KeyedStateHandle keyedStateHandleStream = FutureUtil.runIfNotDoneAndGet(futureKeyedStreamStateHandles);

				List<OperatorStateHandle> operatorStatesBackend = new ArrayList<>(snapshotInProgressList.size());
				List<OperatorStateHandle> operatorStatesStream = new ArrayList<>(snapshotInProgressList.size());

				for (OperatorSnapshotResult snapshotInProgress : snapshotInProgressList) {
					if (null != snapshotInProgress) {
						operatorStatesBackend.add(
							FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateManagedFuture()));
						operatorStatesStream.add(
							FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateRawFuture()));
					} else {
						operatorStatesBackend.add(null);
						operatorStatesStream.add(null);
					}
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState =
					new ChainedStateHandle<>(nonPartitionedStateHandles);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend =
					new ChainedStateHandle<>(operatorStatesBackend);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream =
					new ChainedStateHandle<>(operatorStatesStream);

				SubtaskState subtaskState = createSubtaskStateFromSnapshotStateHandles(
					chainedNonPartitionedOperatorsState,
					chainedOperatorStateBackend,
					chainedOperatorStateStream,
					keyedStateHandleBackend,
					keyedStateHandleStream);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING,
					CheckpointingOperation.AsynCheckpointState.COMPLETED)) {

					final long syncDurationMillis = checkpointMetrics.getSyncDurationMillis();
					final long alignemnt = checkpointMetrics.getAlignmentDurationNanos() / 1_000_000;
					final long bytesAlignment = checkpointMetrics.getBytesBufferedInAlignment();

					TaskMetricGroup tmGroup = owner.getEnvironment().getMetricGroup();

					tmGroup.gauge(ASYNC_DURATION_MILLIS, new Gauge<Long>() {
						@Override
						public Long getValue() {
							return asyncDurationMillis;
						}
					});

					tmGroup.gauge(SYNC_DURATION_MILLIS, new Gauge<Long>() {
						@Override
						public Long getValue() {
							return syncDurationMillis;
						}
					});


					tmGroup.gauge(ALIGNMENT_DURATION_MILLIS, new Gauge<Long>() {
						@Override
						public Long getValue() {
							return alignemnt;
						}
					});


					tmGroup.gauge(BYTES_ALIGNMENT, new Gauge<Long>() {
						@Override
						public Long getValue() {
							return bytesAlignment;
						}
					});

					owner.getEnvironment().acknowledgeCheckpoint(
						checkpointMetaData.getCheckpointId(),
						checkpointMetrics,
						subtaskState);

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
							owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);
					}
				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				// the state is completed if an exception occurred in the acknowledgeCheckpoint call
				// in order to clean up, we have to set it to RUNNING again.
				asyncCheckpointState.compareAndSet(
					CheckpointingOperation.AsynCheckpointState.COMPLETED,
					CheckpointingOperation.AsynCheckpointState.RUNNING);

				try {
					cleanup();
				} catch (Exception cleanupException) {
					e.addSuppressed(cleanupException);
				}

				// registers the exception and tries to fail the whole task
				AsynchronousException asyncException = new AsynchronousException(
					new Exception(
						"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() +
							" for operator " + owner.getName() + '.',
						e));

				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			} finally {
				owner.cancelables.unregisterClosable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
			}
		}

		@Override
		public void close() {
			try {
				cleanup();
			} catch (Exception cleanupException) {
				LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
			}
		}

		private SubtaskState createSubtaskStateFromSnapshotStateHandles(
			ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream,
			KeyedStateHandle keyedStateHandleBackend,
			KeyedStateHandle keyedStateHandleStream) {

			boolean hasAnyState = keyedStateHandleBackend != null
				|| keyedStateHandleStream != null
				|| !chainedOperatorStateBackend.isEmpty()
				|| !chainedOperatorStateStream.isEmpty()
				|| !chainedNonPartitionedOperatorsState.isEmpty();

			// we signal a stateless task by reporting null, so that there are no attempts to assign empty state to
			// stateless tasks on restore. This allows for simple job modifications that only concern stateless without
			// the need to assign them uids to match their (always empty) states.
			return hasAnyState ? new SubtaskState(
				chainedNonPartitionedOperatorsState,
				chainedOperatorStateBackend,
				chainedOperatorStateStream,
				keyedStateHandleBackend,
				keyedStateHandleStream)
				: null;
		}

		private void cleanup() throws Exception {
			if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING, CheckpointingOperation.AsynCheckpointState.DISCARDED)) {
				LOG.debug("Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.", checkpointMetaData.getCheckpointId(), owner.getName());
				Exception exception = null;

				// clean up ongoing operator snapshot results and non partitioned state handles
				for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
					if (operatorSnapshotResult != null) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception cancelException) {
							exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
						}
					}
				}

				// discard non partitioned state handles
				try {
					StateUtil.bestEffortDiscardAllStateObjects(nonPartitionedStateHandles);
				} catch (Exception discardException) {
					exception = ExceptionUtils.firstOrSuppressed(discardException, exception);
				}

				if (null != exception) {
					throw exception;
				}
			} else {
				LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
						"already been completed. Thus, the state handles are not cleaned up.",
					owner.getName(),
					checkpointMetaData.getCheckpointId());
			}
		}
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	private static final class CheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private final List<StreamStateHandle> nonPartitionedStates;
		private final List<OperatorSnapshotResult> snapshotInProgressList;

		public CheckpointingOperation(
			StreamTask<?, ?> owner,
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.nonPartitionedStates = new ArrayList<>(allOperators.length);
			this.snapshotInProgressList = new ArrayList<>(allOperators.length);
		}

		public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			boolean failed = true;
			try {
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// at this point we are transferring ownership over snapshotInProgressList for cleanup to the thread
				runAsyncCheckpointingAndAcknowledge();
				failed = false;

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} finally {
				if (failed) {
					// Cleanup to release resources
					for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
						if (null != operatorSnapshotResult) {
							try {
								operatorSnapshotResult.cancel();
							} catch (Exception e) {
								LOG.warn("Could not properly cancel an operator snapshot result.", e);
							}
						}
					}

					// Cleanup non partitioned state handles
					for (StreamStateHandle nonPartitionedState : nonPartitionedStates) {
						if (nonPartitionedState != null) {
							try {
								nonPartitionedState.discardState();
							} catch (Exception e) {
								LOG.warn("Could not properly discard a non partitioned " +
									"state. This might leave some orphaned files behind.", e);
							}
						}
					}

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - did NOT finish synchronous part of checkpoint {}." +
								"Alignment duration: {} ms, snapshot duration {} ms",
							owner.getName(), checkpointMetaData.getCheckpointId(),
							checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
							checkpointMetrics.getSyncDurationMillis());
					}
				}
			}
		}

		@SuppressWarnings("deprecation")
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {
				// first call the legacy checkpoint code paths
				nonPartitionedStates.add(op.snapshotLegacyOperatorState(
					checkpointMetaData.getCheckpointId(),
					checkpointMetaData.getTimestamp(),
					checkpointOptions));

				OperatorSnapshotResult snapshotInProgress = op.snapshotState(
					checkpointMetaData.getCheckpointId(),
					checkpointMetaData.getTimestamp(),
					checkpointOptions);

				snapshotInProgressList.add(snapshotInProgress);
			} else {
				nonPartitionedStates.add(null);
				OperatorSnapshotResult emptySnapshotInProgress = new OperatorSnapshotResult();
				snapshotInProgressList.add(emptySnapshotInProgress);
			}
		}

		public void runAsyncCheckpointingAndAcknowledge() throws IOException {

			AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
				owner,
				nonPartitionedStates,
				snapshotInProgressList,
				checkpointMetaData,
				checkpointMetrics,
				startAsyncPartNano);

			owner.cancelables.registerClosable(asyncCheckpointRunnable);
			owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
		}

		private enum AsynCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}

	private static final class SynchronousCheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final StateMigrationMetaData stateMigrationMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;

		// ------------------------

		private final List<StreamStateHandle> nonPartitionedStates;
		private final List<OperatorSnapshotResult> snapshotInProgressList;

		public SynchronousCheckpointingOperation(
			StreamTask<?, ?> owner,
			StateMigrationMetaData stateMigrationMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.stateMigrationMetaData = Preconditions.checkNotNull(stateMigrationMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.nonPartitionedStates = new ArrayList<>(allOperators.length);
			this.snapshotInProgressList = new ArrayList<>(allOperators.length);
		}

		public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			boolean failed = true;
			try {
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				LOG.error("BENCHMARK:Operator {} ({}) is now starting asynchronous checkpoint part",
					owner.getName(), owner.getEnvironment().getExecutionId());

				LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
					stateMigrationMetaData.getStateMigrationId(), owner.getName());

				// at this point we are transferring ownership over snapshotInProgressList for cleanup to the thread
				runSyncCheckpointingAndAcknowledge();
				failed = false;

			} finally {
				if (failed) {
					// Cleanup to release resources
					for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
						if (null != operatorSnapshotResult) {
							try {
								operatorSnapshotResult.cancel();
							} catch (Exception e) {
								LOG.warn("Could not properly cancel an operator snapshot result.", e);
							}
						}
					}

					// Cleanup non partitioned state handles
					for (StreamStateHandle nonPartitionedState : nonPartitionedStates) {
						if (nonPartitionedState != null) {
							try {
								nonPartitionedState.discardState();
							} catch (Exception e) {
								LOG.warn("Could not properly discard a non partitioned " +
									"state. This might leave some orphaned files behind.", e);
							}
						}
					}

					LOG.debug("{} - did NOT finish synchronous part of state migration {}.",
						owner.getName(), stateMigrationMetaData.getStateMigrationId());
				}
			}
		}

		@SuppressWarnings("deprecation")
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {
				// first call the legacy checkpoint code paths
				nonPartitionedStates.add(op.snapshotLegacyOperatorState(
					stateMigrationMetaData.getStateMigrationId(),
					stateMigrationMetaData.getTimestamp(),
					checkpointOptions));

				OperatorSnapshotResult snapshotInProgress = op.snapshotState(
					stateMigrationMetaData.getStateMigrationId(),
					stateMigrationMetaData.getTimestamp(),
					checkpointOptions);

				snapshotInProgressList.add(snapshotInProgress);
			} else {
				nonPartitionedStates.add(null);
				OperatorSnapshotResult emptySnapshotInProgress = new OperatorSnapshotResult();
				snapshotInProgressList.add(emptySnapshotInProgress);
			}
		}

		public void runSyncCheckpointingAndAcknowledge() throws IOException {

			try (SendMigrationState synchronousStateSending = new SendMigrationState(
				owner,
				nonPartitionedStates,
				snapshotInProgressList,
				stateMigrationMetaData,
				checkpointMetrics,
				startSyncPartNano)) {

				synchronousStateSending.execute();

			} finally {
				LOG.debug("Migrated state {} to JobManager for task {}.",
					stateMigrationMetaData.getStateMigrationId(), owner.getName());
			}
		}
	}

	private static final class SendMigrationState implements Closeable {

		private final StreamTask<?, ?> owner;

		private final List<OperatorSnapshotResult> snapshotInProgressList;

		private RunnableFuture<KeyedStateHandle> futureKeyedBackendStateHandles;
		private RunnableFuture<KeyedStateHandle> futureKeyedStreamStateHandles;

		private List<StreamStateHandle> nonPartitionedStateHandles;

		private final StateMigrationMetaData stateMigrationMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;

		private final AtomicReference<CheckpointingOperation.AsynCheckpointState> asyncCheckpointState =
			new AtomicReference<>(CheckpointingOperation.AsynCheckpointState.RUNNING);

		SendMigrationState(
			StreamTask<?, ?> owner,
			List<StreamStateHandle> nonPartitionedStateHandles,
			List<OperatorSnapshotResult> snapshotInProgressList,
			StateMigrationMetaData stateMigrationMetaData,
			CheckpointMetrics checkpointMetrics,
			long syncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.snapshotInProgressList = Preconditions.checkNotNull(snapshotInProgressList);
			this.stateMigrationMetaData = Preconditions.checkNotNull(stateMigrationMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.nonPartitionedStateHandles = nonPartitionedStateHandles;
			this.asyncStartNanos = syncStartNanos;

			if (!snapshotInProgressList.isEmpty()) {
				// TODO Currently only the head operator of a chain can have keyed state, so simply access it directly.
				int headIndex = snapshotInProgressList.size() - 1;
				OperatorSnapshotResult snapshotInProgress = snapshotInProgressList.get(headIndex);
				if (null != snapshotInProgress) {
					this.futureKeyedBackendStateHandles = snapshotInProgress.getKeyedStateManagedFuture();
					this.futureKeyedStreamStateHandles = snapshotInProgress.getKeyedStateRawFuture();
				}
			}
		}

		public void execute() {
			try {
				// Keyed state handle future, currently only one (the head) operator can have this
				KeyedStateHandle keyedStateHandleBackend = FutureUtil.runIfNotDoneAndGet(futureKeyedBackendStateHandles);
				KeyedStateHandle keyedStateHandleStream = FutureUtil.runIfNotDoneAndGet(futureKeyedStreamStateHandles);

				List<OperatorStateHandle> operatorStatesBackend = new ArrayList<>(snapshotInProgressList.size());
				List<OperatorStateHandle> operatorStatesStream = new ArrayList<>(snapshotInProgressList.size());

				for (OperatorSnapshotResult snapshotInProgress : snapshotInProgressList) {
					if (null != snapshotInProgress) {
						operatorStatesBackend.add(
							FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateManagedFuture()));
						operatorStatesStream.add(
							FutureUtil.runIfNotDoneAndGet(snapshotInProgress.getOperatorStateRawFuture()));
					} else {
						operatorStatesBackend.add(null);
						operatorStatesStream.add(null);
					}
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState =
					new ChainedStateHandle<>(nonPartitionedStateHandles);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend =
					new ChainedStateHandle<>(operatorStatesBackend);

				ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream =
					new ChainedStateHandle<>(operatorStatesStream);

				SubtaskState subtaskState = createSubtaskStateFromSnapshotStateHandles(
					chainedNonPartitionedOperatorsState,
					chainedOperatorStateBackend,
					chainedOperatorStateStream,
					keyedStateHandleBackend,
					keyedStateHandleStream);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING,
					CheckpointingOperation.AsynCheckpointState.COMPLETED)) {

					LOG.error("BENCHMARK:Operator {} ({}) is now completed asynchronous checkpoint part with size {}",
						owner.getName(), owner.getEnvironment().getExecutionId(), subtaskState.getStateSize());

					owner.getEnvironment().acknowledgeStateMigration(
						stateMigrationMetaData.getStateMigrationId(),
						checkpointMetrics,
						subtaskState);

					if (LOG.isDebugEnabled()) {
						LOG.debug("{} - finished SendMigrationState {}. Duration: {} ms",
							owner.getName(), stateMigrationMetaData.getStateMigrationId(), asyncDurationMillis);
					}
				} else {
					LOG.debug("{} - SendMigrationState {} could not be completed because it was closed before.",
						owner.getName(),
						stateMigrationMetaData.getStateMigrationId());
				}
			} catch (Exception e) {
				// the state is completed if an exception occurred in the acknowledgeCheckpoint call
				// in order to clean up, we have to set it to RUNNING again.
				asyncCheckpointState.compareAndSet(
					CheckpointingOperation.AsynCheckpointState.COMPLETED,
					CheckpointingOperation.AsynCheckpointState.RUNNING);

				try {
					cleanup();
				} catch (Exception cleanupException) {
					e.addSuppressed(cleanupException);
				}

				// registers the exception and tries to fail the whole task
				AsynchronousException asyncException = new AsynchronousException(
					new Exception(
						"Could not materialize checkpoint " + stateMigrationMetaData.getStateMigrationId() +
							" for operator " + owner.getName() + '.',
						e));

				owner.handleAsyncException("Failure in SendMigrationState materialization", asyncException);
			} finally {
				owner.cancelables.unregisterClosable(this);
			}
		}

		@Override
		public void close() {
			try {
				cleanup();
			} catch (Exception cleanupException) {
				LOG.warn("Could not properly clean up SendMigrationState.", cleanupException);
			}
		}

		private SubtaskState createSubtaskStateFromSnapshotStateHandles(
			ChainedStateHandle<StreamStateHandle> chainedNonPartitionedOperatorsState,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateBackend,
			ChainedStateHandle<OperatorStateHandle> chainedOperatorStateStream,
			KeyedStateHandle keyedStateHandleBackend,
			KeyedStateHandle keyedStateHandleStream) {

			boolean hasAnyState = keyedStateHandleBackend != null
				|| keyedStateHandleStream != null
				|| !chainedOperatorStateBackend.isEmpty()
				|| !chainedOperatorStateStream.isEmpty()
				|| !chainedNonPartitionedOperatorsState.isEmpty();

			// we signal a stateless task by reporting null, so that there are no attempts to assign empty state to
			// stateless tasks on restore. This allows for simple job modifications that only concern stateless without
			// the need to assign them uids to match their (always empty) states.
			return hasAnyState ? new SubtaskState(
				chainedNonPartitionedOperatorsState,
				chainedOperatorStateBackend,
				chainedOperatorStateStream,
				keyedStateHandleBackend,
				keyedStateHandleStream)
				: null;
		}

		private void cleanup() throws Exception {
			if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsynCheckpointState.RUNNING, CheckpointingOperation.AsynCheckpointState.DISCARDED)) {
				LOG.debug("Cleanup SendMigrationState for checkpoint {} of {}.", stateMigrationMetaData.getStateMigrationId(), owner.getName());
				Exception exception = null;

				// clean up ongoing operator snapshot results and non partitioned state handles
				for (OperatorSnapshotResult operatorSnapshotResult : snapshotInProgressList) {
					if (operatorSnapshotResult != null) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception cancelException) {
							exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
						}
					}
				}

				// discard non partitioned state handles
				try {
					StateUtil.bestEffortDiscardAllStateObjects(nonPartitionedStateHandles);
				} catch (Exception discardException) {
					exception = ExceptionUtils.firstOrSuppressed(discardException, exception);
				}

				if (null != exception) {
					throw exception;
				}
			} else {
				LOG.debug("{} - SendMigrationState for migration {} has " +
						"already been completed. Thus, the state handles are not cleaned up.",
					owner.getName(),
					stateMigrationMetaData.getStateMigrationId());
			}
		}
	}
}
