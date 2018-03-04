/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.PausingOperatorMarker;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p> Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p> As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p> In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
public class SingleInputGate implements InputGate {

	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	/** Lock object to guard partition requests and runtime channel updates. */
	private final Object requestLock = new Object();

	/** The name of the owning task, for logging purposes. */
	final String owningTaskName;

	/** The job ID of the owning task. */
	private final JobID jobId;

	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is consuming. */
	private final ResultPartitionType consumedPartitionType;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int consumedSubpartitionIndex;

	/** The number of input channels (equivalent to the number of consumed partitions). */
	private final int numberOfInputChannels;

	/**
	 * Input channels. There is a one input channel for each consumed intermediate result partition.
	 * We store this in a map for runtime updates of single channels.
	 */
	private final Map<IntermediateResultPartitionID, InputChannel> inputChannels;

	private final Map<Integer, IntermediateResultPartitionID> inputChannelsToIndex;

	/** Channels, which notified this input gate about available data. */
	private final ArrayDeque<InputChannel> inputChannelsWithData = new ArrayDeque<>();

	private final BitSet channelsWithEndOfPartitionEvents;

	/** The partition state listener listening to failed partition requests. */
	private final TaskActions taskActions;

	/**
	 * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
	 * from this pool.
	 */
	private BufferPool bufferPool;

	private boolean hasReceivedAllEndOfPartitionEvents;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	/** Flag indicating whether all resources have been released. */
	private volatile boolean isReleased;

	/** Registered listener to forward buffer notifications to. */
	private volatile InputGateListener inputGateListener;

	private final List<TaskEvent> pendingEvents = new ArrayList<>();

	private int numberOfUninitializedChannels;

	/** A timer to retrigger local partition requests. Only initialized if actually needed. */
	private Timer retriggerLocalRequestTimer;

	public SingleInputGate(
		String owningTaskName,
		JobID jobId,
		IntermediateDataSetID consumedResultId,
		final ResultPartitionType consumedPartitionType,
		int consumedSubpartitionIndex,
		int numberOfInputChannels,
		TaskActions taskActions,
		TaskIOMetricGroup metrics) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.jobId = checkNotNull(jobId);

		this.consumedResultId = checkNotNull(consumedResultId);
		this.consumedPartitionType = checkNotNull(consumedPartitionType);

		checkArgument(consumedSubpartitionIndex >= 0);
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;

		checkArgument(numberOfInputChannels > 0);
		this.numberOfInputChannels = numberOfInputChannels;

		this.inputChannels = new HashMap<>(numberOfInputChannels);
		this.inputChannelsToIndex = new HashMap<Integer, IntermediateResultPartitionID>(numberOfInputChannels);
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);

		this.taskActions = checkNotNull(taskActions);
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberOfInputChannels() {
		return numberOfInputChannels;
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	@Override
	public int getPageSize() {
		if (bufferPool != null) {
			return bufferPool.getMemorySegmentSize();
		}
		else {
			throw new IllegalStateException("Input gate has not been initialized with buffers.");
		}
	}

	public int getNumberOfQueuedBuffers() {
		// re-try 3 times, if fails, return 0 for "unknown"
		for (int retry = 0; retry < 3; retry++) {
			try {
				int totalBuffers = 0;

				for (InputChannel channel : inputChannels.values()) {
					if (channel instanceof RemoteInputChannel) {
						totalBuffers += ((RemoteInputChannel) channel).getNumberOfQueuedBuffers();
					}
				}

				return  totalBuffers;
			}
			catch (Exception ignored) {}
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	// Setup/Life-cycle
	// ------------------------------------------------------------------------

	public void setBufferPool(BufferPool bufferPool) {
		// Sanity checks
		checkArgument(numberOfInputChannels == bufferPool.getNumberOfRequiredMemorySegments(),
				"Bug in input gate setup logic: buffer pool has not enough guaranteed buffers " +
						"for this input gate. Input gates require at least as many buffers as " +
						"there are input channels.");

		checkState(this.bufferPool == null, "Bug in input gate setup logic: buffer pool has" +
				"already been set for this input gate.");

		this.bufferPool = checkNotNull(bufferPool);
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel) {
		setInputChannel(partitionId, inputChannel, -1);
	}

	public void setInputChannel(IntermediateResultPartitionID partitionId, InputChannel inputChannel, int index) {
		synchronized (requestLock) {

			inputChannelsToIndex.put(index, partitionId);

			if (inputChannels.put(checkNotNull(partitionId), checkNotNull(inputChannel)) == null
					&& inputChannel.getClass() == UnknownInputChannel.class) {

				numberOfUninitializedChannels++;
			}
		}
	}

	public void updateInputChannel(InputChannelDeploymentDescriptor icdd) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (isReleased) {
				// There was a race with a task failure/cancel
				return;
			}

			final IntermediateResultPartitionID partitionId = icdd.getConsumedPartitionId().getPartitionId();

			InputChannel current = inputChannels.get(partitionId);

			if (current.getClass() == UnknownInputChannel.class) {

				UnknownInputChannel unknownChannel = (UnknownInputChannel) current;

				InputChannel newChannel;

				ResultPartitionLocation partitionLocation = icdd.getConsumedPartitionLocation();

				if (partitionLocation.isLocal()) {
					newChannel = unknownChannel.toLocalInputChannel();
				}
				else if (partitionLocation.isRemote()) {
					newChannel = unknownChannel.toRemoteInputChannel(partitionLocation.getConnectionId());
				}
				else {
					throw new IllegalStateException("Tried to update unknown channel with unknown channel.");
				}

				LOG.debug("Updated unknown input channel to {}.", newChannel);

				inputChannels.put(partitionId, newChannel);

				if (requestedPartitionsFlag) {
					newChannel.requestSubpartition(consumedSubpartitionIndex);
				}

				for (TaskEvent event : pendingEvents) {
					newChannel.sendTaskEvent(event);
				}

				if (--numberOfUninitializedChannels == 0) {
					pendingEvents.clear();
				}
			}
		}
	}

	/**
	 * Retriggers a partition request.
	 */
	public void retriggerPartitionRequest(IntermediateResultPartitionID partitionId) throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!isReleased) {
				final InputChannel ch = inputChannels.get(partitionId);

				checkNotNull(ch, "Unknown input channel with ID " + partitionId);

				LOG.debug("Retriggering partition request {}:{}.", ch.partitionId, consumedSubpartitionIndex);

				if (ch.getClass() == RemoteInputChannel.class) {
					final RemoteInputChannel rch = (RemoteInputChannel) ch;
					rch.retriggerSubpartitionRequest(consumedSubpartitionIndex);
				}
				else if (ch.getClass() == LocalInputChannel.class) {
					final LocalInputChannel ich = (LocalInputChannel) ch;

					if (retriggerLocalRequestTimer == null) {
						retriggerLocalRequestTimer = new Timer(true);
					}

					ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer, consumedSubpartitionIndex);
				}
				else {
					throw new IllegalStateException(
							"Unexpected type of channel to retrigger partition: " + ch.getClass());
				}
			}
		}
	}

	public void releaseAllResources() throws IOException {
		boolean released = false;
		synchronized (requestLock) {
			if (!isReleased) {
				try {
					LOG.debug("{}: Releasing {}.", owningTaskName, this);

					if (retriggerLocalRequestTimer != null) {
						retriggerLocalRequestTimer.cancel();
					}

					for (InputChannel inputChannel : inputChannels.values()) {
						try {
							inputChannel.releaseAllResources();
						}
						catch (IOException e) {
							LOG.warn("Error during release of channel resources: " + e.getMessage(), e);
						}
					}

					// The buffer pool can actually be destroyed immediately after the
					// reader received all of the data from the input channels.
					if (bufferPool != null) {
						bufferPool.lazyDestroy();
					}
				}
				finally {
					isReleased = true;
					released = true;
				}
			}
		}

		if (released) {
			synchronized (inputChannelsWithData) {
				inputChannelsWithData.notifyAll();
			}
		}
	}

	@Override
	public boolean isFinished() {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				if (!inputChannel.isReleased()) {
					return false;
				}
			}
		}

		return true;
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (!requestedPartitionsFlag) {
				if (isReleased) {
					throw new IllegalStateException("Already released.");
				}

				// Sanity checks
				if (numberOfInputChannels != inputChannels.size()) {
					throw new IllegalStateException("Bug in input gate setup logic: mismatch between" +
							"number of total input channels and the currently set number of input " +
							"channels.");
				}

				for (InputChannel inputChannel : inputChannels.values()) {
					LOG.info("Task {} requests subpartition with inputChannel {} with index {}",
						owningTaskName, inputChannel, consumedSubpartitionIndex);
					inputChannel.requestSubpartition(consumedSubpartitionIndex);
				}
			}

			requestedPartitionsFlag = true;
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return null;
		}

		if (isReleased) {
			throw new IllegalStateException("Released");
		}

		requestPartitions();

		InputChannel currentChannel;
		boolean moreAvailable;

		synchronized (inputChannelsWithData) {
			while (inputChannelsWithData.size() == 0) {
				if (isReleased) {
					throw new IllegalStateException("Released");
				}

				inputChannelsWithData.wait();
			}

			currentChannel = inputChannelsWithData.remove();
			moreAvailable = inputChannelsWithData.size() > 0;
		}

		final BufferAndAvailability result = currentChannel.getNextBuffer();

		// Sanity check that notifications only happen when data is available
		if (result == null) {
			throw new IllegalStateException("Bug in input gate/channel logic: input gate got " +
					"notified by channel about available data, but none was available.");
		}

		// this channel was now removed from the non-empty channels queue
		// we re-add it in case it has more data, because in that case no "non-empty" notification
		// will come for that channel
		if (result.moreAvailable()) {
			queueChannel(currentChannel);
		}

		final Buffer buffer = result.buffer();
		if (buffer.isBuffer()) {
			return new BufferOrEvent(buffer, currentChannel.getChannelIndex(), moreAvailable);
		}
		else {
			final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

			if (event.getClass() == EndOfPartitionEvent.class) {
				channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());

				if (channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels) {
					hasReceivedAllEndOfPartitionEvents = true;
				}

				currentChannel.notifySubpartitionConsumed();

				currentChannel.releaseAllResources();
			}

			return new BufferOrEvent(event, currentChannel.getChannelIndex(), moreAvailable);
		}
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		synchronized (requestLock) {
			for (InputChannel inputChannel : inputChannels.values()) {
				inputChannel.sendTaskEvent(event);
			}

			if (numberOfUninitializedChannels > 0) {
				pendingEvents.add(event);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Channel notifications
	// ------------------------------------------------------------------------

	@Override
	public void registerListener(InputGateListener inputGateListener) {
		if (this.inputGateListener == null) {
			this.inputGateListener = inputGateListener;
		} else {
			throw new IllegalStateException("Multiple listeners");
		}
	}

	void notifyChannelNonEmpty(InputChannel channel) {
		queueChannel(checkNotNull(channel));
	}

	void triggerPartitionStateCheck(ResultPartitionID partitionId) {
		taskActions.triggerPartitionProducerStateCheck(jobId, consumedResultId, partitionId);
	}

	private void queueChannel(InputChannel channel) {
		int availableChannels;

		synchronized (inputChannelsWithData) {
			availableChannels = inputChannelsWithData.size();

			inputChannelsWithData.add(channel);

			if (availableChannels == 0) {
				inputChannelsWithData.notifyAll();
			}
		}

		if (availableChannels == 0) {
			InputGateListener listener = inputGateListener;
			if (listener != null) {
				listener.notifyInputGateNonEmpty(this);
			}
		}
	}

	// ------------------------------------------------------------------------

	public Map<IntermediateResultPartitionID, InputChannel> getInputChannels() {
		return inputChannels;
	}

	public Map<Integer, IntermediateResultPartitionID> getInputChannelIndices() {
		return inputChannelsToIndex;
	}

	// ------------------------------------------------------------------------

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public static SingleInputGate create(
		String owningTaskName,
		JobID jobId,
		ExecutionAttemptID executionId,
		InputGateDeploymentDescriptor igdd,
		NetworkEnvironment networkEnvironment,
		TaskActions taskActions,
		TaskIOMetricGroup metrics) {

		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length, taskActions, metrics);

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics,
					owningTaskName
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i], i);
		}

		LOG.debug("Task {} created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public static SingleInputGate createModified(
		String owningTaskName,
		JobID jobId,
		ExecutionAttemptID executionId,
		InputGateDeploymentDescriptor igdd,
		NetworkEnvironment networkEnvironment,
		TaskActions taskActions,
		TaskIOMetricGroup metrics,
		ExecutionAttemptID migratedMapTask,
		TaskManagerLocation newLocation,
		int mapSubtaskIndex) {

		LOG.debug("Task {} creates modified input with location {} and index {}.", owningTaskName, newLocation, mapSubtaskIndex);


		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length, taskActions, metrics);

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			LOG.debug("Task {} creating input channel {} with partition location.", owningTaskName, i, partitionLocation);

			if (i == mapSubtaskIndex) {

				final ResultPartitionID changedPartitionId =
					new ResultPartitionID(partitionId.getPartitionId(), migratedMapTask);

				LOG.debug("Modified ResultPartitionID {}", changedPartitionId);


				final ConnectionID connectionID =
					new ConnectionID(newLocation, partitionLocation
						.getConnectionId()
						.getConnectionIndex());

				inputChannels[i] = new RemoteInputChannel(inputGate, i, changedPartitionId,
					connectionID,
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics,
					owningTaskName
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics,
					owningTaskName
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i], i);
		}

		LOG.debug("Task {} created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public static SingleInputGate createIncreasedDoP(
		String owningTaskName,
		JobID jobId,
		ExecutionAttemptID executionId,
		InputGateDeploymentDescriptor igdd,
		NetworkEnvironment networkEnvironment,
		TaskActions taskActions,
		TaskIOMetricGroup metrics,
		ExecutionAttemptID newlyStarteFilterTask,
		boolean local,
		TaskManagerLocation filterTMLocation,
		IntermediateResultPartitionID newPartitionID,
		int mapSubtaskIndex,
		int connectionIndex) {

		LOG.debug("Task {} creates modified input with location {} and index {}.", owningTaskName, filterTMLocation, mapSubtaskIndex);

		final IntermediateDataSetID consumedResultId = checkNotNull(igdd.getConsumedResultId());
		final ResultPartitionType consumedPartitionType = checkNotNull(igdd.getConsumedPartitionType());

		final int consumedSubpartitionIndex = igdd.getConsumedSubpartitionIndex();
		checkArgument(consumedSubpartitionIndex >= 0);

		final InputChannelDeploymentDescriptor[] icdd = checkNotNull(igdd.getInputChannelDeploymentDescriptors());

		final SingleInputGate inputGate = new SingleInputGate(
			owningTaskName, jobId, consumedResultId, consumedPartitionType, consumedSubpartitionIndex,
			icdd.length + 1, taskActions, metrics);

		// TODO Masterthesis InputChannel
		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[icdd.length + 1];

		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;

		for (int i = 0; i < inputChannels.length - 1; i++) {
			final ResultPartitionID partitionId = icdd[i].getConsumedPartitionId();
			final ResultPartitionLocation partitionLocation = icdd[i].getConsumedPartitionLocation();

			LOG.debug("Task {} creating input channel {} with partition location.", owningTaskName, i, partitionLocation);

			if (partitionLocation.isLocal()) {
				inputChannels[i] = new LocalInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numLocalChannels++;
			}
			else if (partitionLocation.isRemote()) {
				inputChannels[i] = new RemoteInputChannel(inputGate, i, partitionId,
					partitionLocation.getConnectionId(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics,
					owningTaskName
				);

				numRemoteChannels++;
			}
			else if (partitionLocation.isUnknown()) {
				inputChannels[i] = new UnknownInputChannel(inputGate, i, partitionId,
					networkEnvironment.getResultPartitionManager(),
					networkEnvironment.getTaskEventDispatcher(),
					networkEnvironment.getConnectionManager(),
					networkEnvironment.getPartitionRequestInitialBackoff(),
					networkEnvironment.getPartitionRequestMaxBackoff(),
					metrics
				);

				numUnknownChannels++;
			}
			else {
				throw new IllegalStateException("Unexpected partition location.");
			}

			inputGate.setInputChannel(partitionId.getPartitionId(), inputChannels[i], i);
		}

//		final ResultPartitionID partitionId = icdd[0].getConsumedPartitionId();
//		final ResultPartitionID changedPartitionId = new ResultPartitionID(partitionId.getPartitionId(), newlyStarteFilterTask);
		final ResultPartitionID changedPartitionId = new ResultPartitionID(newPartitionID, newlyStarteFilterTask);
		final ResultPartitionLocation partitionLocation = icdd[0].getConsumedPartitionLocation();

		LOG.debug("Modified ResultPartitionID {}", changedPartitionId);

		// TODO Masterthesis - Differentiate between local and remote input gate

		if (local) {
			inputChannels[inputChannels.length - 1] = new LocalInputChannel(inputGate, 2, changedPartitionId,
				networkEnvironment.getResultPartitionManager(),
				networkEnvironment.getTaskEventDispatcher(),
				networkEnvironment.getPartitionRequestInitialBackoff(),
				networkEnvironment.getPartitionRequestMaxBackoff(),
				metrics
			);
		} else {
			final ConnectionID connectionID =
				new ConnectionID(filterTMLocation, connectionIndex);

			inputChannels[inputChannels.length - 1] = new RemoteInputChannel(inputGate, 2, changedPartitionId,
				connectionID,
				networkEnvironment.getConnectionManager(),
				networkEnvironment.getPartitionRequestInitialBackoff(),
				networkEnvironment.getPartitionRequestMaxBackoff(),
				metrics,
				owningTaskName
			);
		}

		numRemoteChannels++;

//		inputGate.setInputChannel(newPartitionID, inputChannels[inputChannels.length - 1], i);

		LOG.debug("Task {} created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			numLocalChannels,
			numRemoteChannels,
			numUnknownChannels);

		return inputGate;
	}

	@Override
	public String toString() {
		return "InputGate for '" + owningTaskName + "' for IDSID: " + consumedResultId +
			" SubPartitionIndex: " + consumedSubpartitionIndex;
	}

	public void updateInputChannel(IntermediateResultPartitionID partitionId, InputChannel newChannel)
		throws IOException, InterruptedException {
		synchronized (requestLock) {
			if (isReleased) {
				// There was a race with a task failure/cancel
				return;
			}

			InputChannel oldChannel;

			if (inputChannels.containsKey(partitionId)) {
				oldChannel = inputChannels.put(partitionId, newChannel);

				ensureChannelDoesNotContainPendingData(oldChannel);

				oldChannel.releaseAllResources();
			} else {
				throw new RuntimeException("No previous input channel.");
			}

			LOG.error("BENCHMARK: Updated existing input channel to {}.", newChannel);

			newChannel.requestSubpartition(consumedSubpartitionIndex);

//			if (requestedPartitionsFlag) {
//				newChannel.requestSubpartition(consumedSubpartitionIndex);
//			}
//			Use this instead of direct requestSubpartition for exponential backoff
//			retriggerPartitionRequest(partitionId);

			for (TaskEvent event : pendingEvents) {
				newChannel.sendTaskEvent(event);
			}

			if (--numberOfUninitializedChannels == 0) {
				pendingEvents.clear();
			}
		}
	}

	private void ensureChannelDoesNotContainPendingData(InputChannel channel) throws IOException, InterruptedException {
		synchronized (inputChannelsWithData) {

			if (inputChannelsWithData.contains(channel)) {

				String message = "";
				BufferAndAvailability nextBuffer = channel.getNextBuffer();

				message += "More available? " + nextBuffer.moreAvailable();

				Buffer buffer = nextBuffer.buffer();

				if (buffer.isBuffer()) {
					throw new IllegalStateException("Channel " + channel + " should not contain pending data. Data " + buffer + " - " + message);
				} else {
					final AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());

					throw new IllegalStateException("Channel " + channel + " should not contain pending data. Event " + event.getClass() + " - " + event.toString() + " - " + message);
				}
			}
		}
	}
}
