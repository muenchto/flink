package org.apache.flink.streaming.runtime.modification;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.messages.modification.AcknowledgeModification;
import org.apache.flink.runtime.messages.modification.DeclineModification;
import org.apache.flink.runtime.messages.modification.IgnoreModification;
import org.apache.flink.runtime.messages.modification.StateMigrationModification;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.modification.exceptions.OperatorNotFoundException;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ModificationCoordinator {

	public enum ModificationAction {
		PAUSING,
		STOPPING
	}

	private static final long MODIFICATION_TIMEOUT = 30;

	private static final Logger LOG = LoggerFactory.getLogger(ModificationCoordinator.class);

	private final Object lock = new Object();

	private final Object triggerLock = new Object();

	private final AtomicLong modificationIdCounter = new AtomicLong(1);

	private final Map<Long, PendingModification> pendingModifications = new LinkedHashMap<>(16);

	private final Map<Long, CompletedModification> completedModifications = new LinkedHashMap<>(16);

	private final Map<Long, PendingModification> failedModifications = new LinkedHashMap<Long, PendingModification>(16);

	private final Map<ExecutionAttemptID, SubtaskState> storedState = new LinkedHashMap<>(16);

	private final ExecutionGraph executionGraph;

	private final Time rpcCallTimeout;

	private final Collection<BlobKey> blobKeys;

	private final ScheduledThreadPoolExecutor timer;

	private ExecutionAttemptID stoppedMapExecutionAttemptID;

	private int stoppedMapSubTaskIndex;

	public ModificationCoordinator(ExecutionGraph executionGraph, Time rpcCallTimeout) {
		this.executionGraph = Preconditions.checkNotNull(executionGraph);
		this.rpcCallTimeout = rpcCallTimeout;
		this.blobKeys = new HashSet<>();

		this.timer = new ScheduledThreadPoolExecutor(1,
			new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "Modification Timer"));
	}

	public boolean receiveAcknowledgeMessage(AcknowledgeModification message) {

		if (message == null) {
			return false;
		}

		if (message.getJobID() != executionGraph.getJobID()) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job id {}: {}", message.getJobID(), message);
		}

		final long modificationID = message.getModificationID();

		synchronized (lock) {

			final PendingModification modification = pendingModifications.get(modificationID);

			if (modification != null && !modification.isDiscarded()) {

				switch (modification.acknowledgeTask(message.getTaskExecutionId())) {
					case SUCCESS:
						LOG.debug("Received acknowledge message for modification {} from task {} of job {}.",
							modificationID, message.getTaskExecutionId(), message.getJobID());

						if (modification.isFullyAcknowledged()) {
							completePendingCheckpoint(modification);
						}
						break;
					case DUPLICATE:
						LOG.debug("Received a duplicate acknowledge message for modification {}, task {}, job {}.",
							message.getModificationID(), message.getTaskExecutionId(), message.getJobID());
						break;
					case UNKNOWN:
						LOG.warn("Could not acknowledge the modification {} for task {} of job {}, " +
								"because the task's execution attempt id was unknown.",
							message.getModificationID(), message.getTaskExecutionId(), message.getJobID());

						break;
					case DISCARDED:
						LOG.warn("Could not acknowledge the modification {} for task {} of job {}, " +
								"because the pending modification had been discarded",
							message.getModificationID(), message.getTaskExecutionId(), message.getJobID());
				}

				return true;
			} else if (modification != null) {
				// this should not happen
				throw new IllegalStateException(
					"Received message for discarded but non-removed modification " + modificationID);
			} else {
				boolean wasPendingModification;

				// message is for an unknown modification, or comes too late (modification disposed)
				if (completedModifications.containsKey(modificationID)) {
					wasPendingModification = true;
					LOG.warn("Received late message for now expired modification attempt {} from " +
						"{} of job {}.", modificationID, message.getTaskExecutionId(), message.getJobID());
				} else {
					LOG.debug("Received message for an unknown modification {} from {} of job {}.",
						modificationID, message.getTaskExecutionId(), message.getJobID());
					wasPendingModification = false;
				}

				return wasPendingModification;
			}
		}
	}

	/**
	 * Try to complete the given pending modification.
	 * <p>
	 * Important: This method should only be called in the checkpoint lock scope.
	 *
	 * @param pendingModification to complete
	 */
	@GuardedBy("lock")
	private void completePendingCheckpoint(PendingModification pendingModification) {

		assert (Thread.holdsLock(lock));

		final long checkpointId = pendingModification.getModificationId();

		CompletedModification completedModification = pendingModification.finalizeCheckpoint();

		pendingModifications.remove(pendingModification.getModificationId());

		if (completedModification != null) {
			Preconditions.checkState(pendingModification.isFullyAcknowledged());
			completedModifications.put(pendingModification.getModificationId(), completedModification);

			LOG.info("Completed modification {} ({}) in {} ms.",
				pendingModification.getModificationDescription(), checkpointId, completedModification.getDuration());
		} else {
			failedModifications.put(pendingModification.getModificationId(), pendingModification);

			LOG.info("Modification {} ({}) failed.", pendingModification.getModificationDescription(), checkpointId);
		}

		// Maybe modify operators of completed modification
	}

	public void receiveDeclineMessage(DeclineModification message) {

		if (message == null) {
			return;
		}

		if (message.getJobID() != executionGraph.getJobID()) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job id {}: {}", message.getJobID(), message);
		}

		final long modificationID = message.getModificationID();
		final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

		PendingModification pendingModification;

		synchronized (lock) {

			pendingModification = pendingModifications.get(modificationID);

			if (pendingModification != null && !pendingModification.isDiscarded()) {
				LOG.info("Discarding pendingModification {} because of modification decline from task {} : {}",
					modificationID, message.getTaskExecutionId(), reason);

				pendingModifications.remove(modificationID);
				pendingModification.abortDeclined();
				failedModifications.put(modificationID, pendingModification);
			} else if (pendingModification != null) {
				// this should not happen
				throw new IllegalStateException(
					"Received message for discarded but non-removed pendingModification " + modificationID);
			} else {
				if (failedModifications.containsKey(modificationID)) {
					// message is for an unknown pendingModification, or comes too late (pendingModification disposed)
					LOG.debug("Received another decline message for now expired pendingModification attempt {} : {}",
						modificationID, reason);
				} else {
					// message is for an unknown pendingModification. might be so old that we don't even remember it any more
					LOG.debug("Received decline message for unknown (too old?) pendingModification attempt {} : {}",
						modificationID, reason);
				}
			}
		}
	}

	public void receiveIgnoreMessage(IgnoreModification message) {

		if (message == null) {
			return;
		}

		if (message.getJobID() != executionGraph.getJobID()) {
			LOG.error("Received wrong IgnoreModification message for job id {}: {}", message.getJobID(), message);
		}

		final long modificationID = message.getModificationID();

		PendingModification pendingModification;

		synchronized (lock) {

			pendingModification = pendingModifications.get(modificationID);

			if (pendingModification != null && !pendingModification.isDiscarded()) {
				LOG.info("Received ignoring modification for {} from task {}",
					modificationID, message.getTaskExecutionId());
			} else if (pendingModification != null) {
				// this should not happen
				throw new IllegalStateException(
					"Received message for discarded but non-removed pendingModification " + modificationID);
			} else {
				if (failedModifications.containsKey(modificationID)) {
					// message is for an unknown pendingModification, or comes too late (pendingModification disposed)
					LOG.debug("Received another ignore message for now expired pendingModification attempt {} : {}",
						modificationID, message.getTaskExecutionId());
				} else {
					// message is for an unknown pendingModification. might be so old that we don't even remember it any more
					LOG.debug("Received ignore message for unknown (too old?) pendingModification attempt {} : {}",
						modificationID, message.getTaskExecutionId());
				}
			}
		}
	}

	public void receiveStateMigrationMessage(StateMigrationModification message) {

		if (message == null) {
			return;
		}

		if (message.getJobID() != executionGraph.getJobID()) {
			LOG.error("Received wrong StateMigrationModification message for job id {}: {}", message.getJobID(), message);
		}

		final long modificationID = message.getModificationID();

		synchronized (lock) {

			if (storedState.put(message.getTaskExecutionId(), message.getSubtaskState()) != null) {
				LOG.info("Received duplicate StateMigrationModification for {} from task {}. Removed previous.",
					modificationID, message.getTaskExecutionId());
			} else {
				LOG.info("Received valid StateMigrationModification for {} from task {}",
					modificationID, message.getTaskExecutionId());
			}

			PendingModification pendingModification = pendingModifications.get(modificationID);

			if (pendingModification != null && !pendingModification.isDiscarded()) {
				LOG.info("Received ignoring modification for {} from task {}",
					modificationID, message.getTaskExecutionId());
			} else if (pendingModification != null && pendingModification.isDiscarded()) {
				if (failedModifications.containsKey(modificationID)) {
					// message is for an unknown pendingModification, or comes too late (pendingModification disposed)
					LOG.debug("Received another ignore message for now expired StateMigrationModification attempt {} : {}",
						modificationID, message.getTaskExecutionId());
				} else {
					// message is for an unknown pendingModification. might be so old that we don't even remember it any more
					LOG.debug("Received ignore message for unknown (too old?) StateMigrationModification attempt {} : {}",
						modificationID, message.getTaskExecutionId());
				}
			} else {
				LOG.debug("Received message for discarded but non-removed pendingModification {}.", modificationID);
			}
		}
	}

	private void triggerModification(ExecutionJobVertex instancesToPause, final String description, ModificationAction action) {

		ArrayList<ExecutionVertex> objects = new ArrayList<>(instancesToPause.getTaskVertices().length);
		objects.addAll(Arrays.asList(instancesToPause.getTaskVertices()));

//		triggerModification(objects, description, action); // TODO Masterthesis
	}

	private void triggerModification(ArrayList<ExecutionAttemptID> operatorsIdsToSpill,
									 List<ExecutionVertex> subtaskIndicesToPause,
									 final String description,
									 ModificationAction action) {

		Preconditions.checkNotNull(operatorsIdsToSpill);
		Preconditions.checkNotNull(subtaskIndicesToPause);
		Preconditions.checkNotNull(description);
		Preconditions.checkArgument(subtaskIndicesToPause.size() >= 1);
		Preconditions.checkArgument(operatorsIdsToSpill.size() >= 1);

		LOG.info("Triggering modification '{}'.", description);

		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(subtaskIndicesToPause.size());
		Set<Integer> operatorSubTaskIndices = new HashSet<>();

		for (ExecutionVertex executionVertex : subtaskIndicesToPause) {
			ackTasks.put(executionVertex.getCurrentExecutionAttempt().getAttemptId(), executionVertex);
			operatorSubTaskIndices.add(executionVertex.getCurrentExecutionAttempt().getParallelSubtaskIndex());

			if (executionVertex.getExecutionState() != ExecutionState.RUNNING) {
				throw new RuntimeException("ExecutionVertex " + executionVertex + " is not in running state.");
			}
		}

		synchronized (triggerLock) {

			final long modificationId = modificationIdCounter.getAndIncrement();
			final long timestamp = System.currentTimeMillis();

			final PendingModification modification = new PendingModification(
				executionGraph.getJobID(),
				modificationId,
				timestamp,
				ackTasks,
				description);

			// schedule the timer that will clean up the expired checkpoints
			final Runnable canceller = new Runnable() {
				@Override
				public void run() {
					synchronized (lock) {

						LOG.info("Checking if Modification {} ({}) is still ongoing.", description, modificationId);

						// only do the work if the modification is not discarded anyways
						// note that modification completion discards the pending modification object
						if (!modification.isDiscarded()) {
							LOG.info("Modification {} expired before completing.", description);

							modification.abortExpired();
							pendingModifications.remove(modificationId);
							failedModifications.remove(modificationId);
						} else {
							LOG.info("Modification {} already completed.", description);
						}
					}
				}
			};

			try {
				// re-acquire the coordinator-wide lock
				synchronized (lock) {

					LOG.info("Triggering modification {}@{} - {}.", modificationId, timestamp, description);

					pendingModifications.put(modificationId, modification);

					ScheduledFuture<?> cancellerHandle = timer.schedule(
						canceller,
						MODIFICATION_TIMEOUT, TimeUnit.SECONDS);

					modification.setCancellerHandle(cancellerHandle);
				}

				long currentCheckpointID = executionGraph.getCheckpointCoordinator().getCheckpointIdCounter().getCurrent();
				long checkpointIDToModify = currentCheckpointID + 2;

				ExecutionJobVertex source = findSource();

				// send the messages to the tasks that trigger their modification
				for (ExecutionVertex execution: source.getTaskVertices()) {
					execution.getCurrentExecutionAttempt().triggerModification(
						modificationId,
						timestamp,
						new HashSet<ExecutionAttemptID>(operatorsIdsToSpill),
						operatorSubTaskIndices,
						action,
						checkpointIDToModify); // KeySet not serializable
				}

			}
			catch (Throwable t) {
				// guard the map against concurrent modifications
				synchronized (lock) {
					pendingModifications.remove(modificationId);
				}

				if (!modification.isDiscarded()) {
					modification.abortError(new Exception("Failed to trigger modification", t));
				}
			}
		}
	}

	public String getTMDetails() {

		StringBuilder details = new StringBuilder();

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			details.append("\nAttemptID: ")
				.append(executionVertex.getCurrentExecutionAttempt().getAttemptId())
				.append(" - TM ID: ")
				.append(executionVertex.getCurrentAssignedResource().getTaskManagerID())
				.append(" - TM Location: ")
				.append(executionVertex.getCurrentAssignedResource().getTaskManagerLocation())
				.append(" - Name: ")
				.append(executionVertex.getTaskNameWithSubtaskIndex());
		}

		return details.toString();
	}

	private ExecutionVertex getMapExecutionVertexToStop(ResourceID taskManagerId) {
		ExecutionJobVertex map = findMap();

		ExecutionVertex[] taskVertices = map.getTaskVertices();

		for (ExecutionVertex executionVertex : taskVertices) {
			if (executionVertex.getCurrentAssignedResource().getTaskManagerID().equals(taskManagerId)) {
				return executionVertex;
			}
		}

		return null;
	}

	public void increaseDOPOfSink() {
		ExecutionJobVertex sink = findSink();

		ExecutionJobVertex filter = findFilter();
		ExecutionVertex[] taskVertices = filter.getTaskVertices();

		assert taskVertices.length == 3;

		ExecutionVertex taskVertex = taskVertices[2];

		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedPartitions = taskVertex.getProducedPartitions();
		assert producedPartitions.size() == 1;
		IntermediateResultPartitionID irpidOfThirdFilterOperator = producedPartitions.keySet().iterator().next();

		IntermediateResultPartition next = producedPartitions.values().iterator().next();
		int connectionIndex = next.getIntermediateResult().getConnectionIndex();

		TaskManagerLocation filterTMLocation = taskVertex.getCurrentAssignedResource().getTaskManagerLocation();

		sink.getTaskVertices()[0].getCurrentExecutionAttempt().consumeNewProducer(
			rpcCallTimeout,
			taskVertex.getCurrentExecutionAttempt().getAttemptId(),
			irpidOfThirdFilterOperator,
			filterTMLocation,
			connectionIndex,
			2);
	}

	public void increaseDOPOfMap() {
		ExecutionJobVertex map = findMap();

		for (ExecutionVertex executionVertex : map.getTaskVertices()) {
			executionVertex.getCurrentExecutionAttempt().addNewConsumer();
		}
	}

	public void increaseDOPOfFilter() {
		ExecutionJobVertex filter = findFilter();

		ExecutionVertex executionVertex = filter.increaseDegreeOfParallelism(
			rpcCallTimeout,
			executionGraph.getGlobalModVersion(),
			System.currentTimeMillis(),
			executionGraph.getAllIntermediateResults());

		assert filter.getParallelism() == 3;

		ExecutionVertex[] taskVertices = filter.getTaskVertices();

		assert taskVertices.length == 3;

		ExecutionVertex taskVertex = taskVertices[2];

		taskVertex.scheduleForExecution(executionGraph.getSlotProvider(), executionGraph.isQueuedSchedulingAllowed());
	}

	public void restartMapInstance(ResourceID taskmanagerID) {
		ExecutionJobVertex map = findMap();

		ExecutionVertex stoppedExecutionVertex = null;

		for (ExecutionVertex executionVertex : map.getTaskVertices()) {
			if (executionVertex.getCurrentExecutionAttempt().getAttemptId().equals(stoppedMapExecutionAttemptID)) {
				stoppedExecutionVertex = executionVertex;
				break;
			}
		}

		if (stoppedExecutionVertex == null) {
			executionGraph.failGlobal(new RuntimeException("Could not find stopped Map executionVertex"));
			return;
		}

		try {
			stoppedExecutionVertex.resetForNewExecutionModification(
				System.currentTimeMillis(),
				executionGraph.getGlobalModVersion());

			Execution currentExecutionAttempt = stoppedExecutionVertex.getCurrentExecutionAttempt();

			SubtaskState storedState = this.storedState.get(stoppedMapExecutionAttemptID);

			if (storedState == null) {
				throw new IllegalStateException("Could not find state to restore for ExecutionAttempt: "
					+ stoppedMapExecutionAttemptID);
			} else {
				TaskStateHandles taskStateHandles = new TaskStateHandles(storedState);

				currentExecutionAttempt.setInitialState(taskStateHandles);
			}

			currentExecutionAttempt
				.scheduleForMigration(
					executionGraph.getSlotProvider(),
					executionGraph.isQueuedSchedulingAllowed(),
					taskmanagerID);

		} catch (GlobalModVersionMismatch globalModVersionMismatch) {
			executionGraph.failGlobal(globalModVersionMismatch);
			globalModVersionMismatch.printStackTrace();
		}
	}

	public void addedNewOperatorJar(Collection<BlobKey> blobKeys) {
		LOG.debug("Adding BlobKeys {} for executionGraph {}.",
			StringUtils.join(blobKeys, ","),
			executionGraph.getJobID());

		this.blobKeys.addAll(blobKeys);
	}

	public void pauseSink() {
		ExecutionJobVertex sink = findSink();

		triggerModification(sink, "Pause Sink", ModificationAction.PAUSING);
	}

	public void modifySinkInstance() {
		ExecutionJobVertex sink = findSink();

		Preconditions.checkNotNull(sink);

		ExecutionVertex[] taskVertices = findMap().getTaskVertices();
		assert taskVertices != null;
		assert taskVertices.length >= stoppedMapSubTaskIndex;
		Execution newMapExecutionAttemptId = taskVertices[stoppedMapSubTaskIndex].getCurrentExecutionAttempt();
		assert newMapExecutionAttemptId.getAttemptId() != stoppedMapExecutionAttemptID;

		ExecutionVertex[] sinkTaskVertices = sink.getTaskVertices();

		for (ExecutionVertex executionVertex : sinkTaskVertices) {

			List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptor;
			try {
				inputGateDeploymentDescriptor =
					executionVertex.createInputGateDeploymentDescriptor(executionVertex.getCurrentAssignedResource());
			} catch (ExecutionGraphException e) {
				throw new RuntimeException(e);
			}

			executionVertex.getCurrentExecutionAttempt()
				.triggerResumeWithDifferentInputs(
					rpcCallTimeout,
					stoppedMapSubTaskIndex,
					inputGateDeploymentDescriptor);
		}
	}

	public void resumeSink() {
		ExecutionJobVertex source = findSink();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.resumeTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void resumeFilter() {
		ExecutionJobVertex source = findFilter();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.resumeTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void pauseFilter() {
		ExecutionJobVertex filter = findFilter();

		triggerModification(filter, "Pause Filter", ModificationAction.PAUSING);
	}

	public void pauseMap() {
		ExecutionJobVertex map = findMap();

		triggerModification(map, "Pause map", ModificationAction.PAUSING);
	}

	public void pauseMap(ExecutionAttemptID mapAttemptID) {
		ExecutionJobVertex map = findMap();

		ExecutionVertex[] taskVertices = map.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			if (vertex.getCurrentExecutionAttempt().getAttemptId().equals(mapAttemptID)) {

				// TODO Masterthesis Generalize getting of upstream operator

				ArrayList<ExecutionAttemptID> sourceIds = new ArrayList<>();
				for (ExecutionVertex executionVertex : findSource().getTaskVertices()) {
					sourceIds.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
				}

				stoppedMapExecutionAttemptID = vertex.getCurrentExecutionAttempt().getAttemptId();
				stoppedMapSubTaskIndex = vertex.getCurrentExecutionAttempt().getParallelSubtaskIndex();

				triggerModification(sourceIds,
					Collections.singletonList(vertex),
					"Pause single map instance",
					ModificationAction.STOPPING);
				return;
			}
		}

		executionGraph.failGlobal(new Exception("Failed to find map operator instance for " + mapAttemptID));
	}

	public void resumeMapOperator() {
		ExecutionJobVertex source = findMap();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			// TODO Masterthesis: Unnecessary, as getCurrentExecutionAttempt simply returns the last execution.
			// TODO Masterthesis: Therefore, the old attempt never gets resumed
			if (execution.getAttemptId().equals(stoppedMapExecutionAttemptID)) {
				LOG.info("Skipping resuming of map operator for ExecutionAttemptID {}");
				continue;
			}

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.resumeTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void startFilterOperator() {

		ExecutionJobVertex mapOperator = findMap();
		ExecutionJobVertex sourceOperator = findSource();

		if (mapOperator == null) {
			executionGraph.failGlobal(new OperatorNotFoundException("Map", executionGraph.getJobID()));
			return;
		}

		if (sourceOperator == null) {
			executionGraph.failGlobal(new OperatorNotFoundException("Source", executionGraph.getJobID()));
			return;
		}

		List<ExecutionAttemptID> mapExecutionAttemptIDs = new ArrayList<>();
		for (ExecutionVertex executionVertex : mapOperator.getTaskVertices()) {
			mapExecutionAttemptIDs.add(executionVertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// TODO Masterthesis Currently only one ExecutionVertex for the source is supported
		ExecutionVertex[] sourceTaskVertices = sourceOperator.getTaskVertices();

		if (sourceTaskVertices.length != 1) {
			LOG.info("Found {} ExecutionVertices. Expected {}.", sourceTaskVertices.length, 1);
		} else {
			LOG.info("Found {} ExecutionVertices as expected.", sourceTaskVertices.length);
		}

		ExecutionJobVertex filterExecutionJobVertex = buildFilterExecutionJobVertex(sourceOperator, mapOperator);

		if (filterExecutionJobVertex == null) {
			throw new IllegalStateException("Could not create FilterExecutionJobVertex");
		} else {
			LOG.debug("Starting {} instances of the filter operator", filterExecutionJobVertex.getTaskVertices().length);
		}

		LOG.debug("SourceOperator");
		for (ExecutionVertex executionVertex : sourceOperator.getTaskVertices()) {
			LOG.debug("Found ExecutionVertex {} with location {}", executionVertex, executionVertex.getCurrentAssignedResourceLocation());
		}

		LOG.debug("MapOperator");
		for (ExecutionVertex executionVertex : mapOperator.getTaskVertices()) {
			LOG.debug("Found ExecutionVertex {} with location {}", executionVertex, executionVertex.getCurrentAssignedResourceLocation());
		}

		LOG.debug("FilterOperator");
		for (ExecutionVertex executionVertex : filterExecutionJobVertex.getTaskVertices()) {
			LOG.debug("Found ExecutionVertex {} with location {}", executionVertex, executionVertex.getCurrentAssignedResourceLocation());
		}

		Configuration sourceConfiguration = sourceOperator.getJobVertex().getConfiguration();
		StreamConfig sourceStreamConfig = new StreamConfig(sourceConfiguration);
		List<StreamEdge> outEdges = sourceStreamConfig.getOutEdges(executionGraph.getUserClassLoader());

		LOG.debug("Found outEdges for SourceNode: {}", Joiner.on(",").join(outEdges));

		Configuration configuration = filterExecutionJobVertex.getJobVertex().getConfiguration();
		StreamConfig filterStreamConfig = new StreamConfig(configuration);

		filterStreamConfig.setNonChainedOutputs(sourceStreamConfig.getNonChainedOutputs(executionGraph.getUserClassLoader()));
		filterStreamConfig.setOutEdges(sourceStreamConfig.getOutEdges(executionGraph.getUserClassLoader()));
		filterStreamConfig.setVertexID(42);
		filterStreamConfig.setTypeSerializerIn1(sourceStreamConfig.getTypeSerializerIn1(executionGraph.getUserClassLoader()));
		filterStreamConfig.setTypeSerializerOut(sourceStreamConfig.getTypeSerializerOut(executionGraph.getUserClassLoader()));
		filterStreamConfig.setNumberOfInputs(1);
		filterStreamConfig.setNumberOfOutputs(1);
		filterStreamConfig.setTransitiveChainedTaskConfigs(sourceStreamConfig.getTransitiveChainedTaskConfigs(executionGraph.getUserClassLoader()));
		filterStreamConfig.setOutEdgesInOrder(sourceStreamConfig.getOutEdgesInOrder(executionGraph.getUserClassLoader()));
		filterStreamConfig.setChainedOutputs(sourceStreamConfig.getChainedOutputs(executionGraph.getUserClassLoader()));

		for (ExecutionVertex executionVertex : filterExecutionJobVertex.getTaskVertices()) {
			boolean successful = executionVertex.getCurrentExecutionAttempt()
				.scheduleForRuntimeExecution(
					executionGraph.getSlotProvider(),
					executionGraph.isQueuedSchedulingAllowed());
		}
	}


	public String getDetails() throws JobException {

		StringBuilder currentPlan = new StringBuilder();

		currentPlan.append(executionGraph.getJsonPlan()).append("\n");

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			currentPlan.append(ejv.generateDebugString()).append("\n");
		}

		JobInformation jobInformation = executionGraph.getJobInformation();

		currentPlan.append("JobInfo: ").append(jobInformation).append(jobInformation.getJobConfiguration()).append("\n");

		for (Map.Entry<JobVertexID, ExecutionJobVertex> vertex : executionGraph.getTasks().entrySet()) {
			currentPlan
				.append("Vertex:")
				.append(vertex.getKey()).append(" - ")
				.append(vertex.getValue().generateDebugString())
				.append(vertex.getValue().getAggregateState())
				.append(" Outputs: ")
				.append(Arrays.toString(vertex.getValue().getProducedDataSets()))
				.append(" JobVertex: ")
				.append(vertex.getValue().getJobVertex())
				.append(" Inputs: ")
				.append(Joiner.on(",").join(vertex.getValue().getInputs()))
				.append(" IntermediateResultPartition: ");

			for (IntermediateResult result : vertex.getValue().getInputs()) {
				for (IntermediateResultPartition partition : result.getPartitions()) {
					currentPlan
						.append(" PartitionId: ")
						.append(partition.getPartitionId())
						.append(" PartitionNumber: ")
						.append(partition.getPartitionNumber())
						.append("\n");
				}
			}

			currentPlan.append("\n");
		}

		for (Map.Entry<ExecutionAttemptID, Execution> exec : executionGraph.getRegisteredExecutions().entrySet()) {
			currentPlan.append(exec.getKey())
				.append(exec.getValue())
				.append(exec.getValue().getVertexWithAttempt())
				.append(exec.getValue().getAssignedResourceLocation())
				.append(" InvokableName: ")
				.append(exec.getValue().getVertex().getJobVertex().getJobVertex().getInvokableClassName())
				.append("\n");
		}

		currentPlan.append("numVerticesTotal: ").append(executionGraph.getTotalNumberOfVertices());
		currentPlan.append("finishedVertices: ").append(executionGraph.getVerticesFinished());

		return currentPlan.toString();
	}

	public ExecutionJobVertex findFilter() {

		ExecutionJobVertex executionJobVertex = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			// TODO Masterthesis Currently hardcoded
			if (ejv.getJobVertex().getName().toLowerCase().contains("filter")) {
				executionJobVertex = ejv;
			}
		}

		if (executionJobVertex == null) {
			executionGraph.failGlobal(new ExecutionGraphException("Could not find filter"));
			throw new RuntimeException("Could not find filter");
		} else {
			return executionJobVertex;
		}
	}

	public ExecutionJobVertex findSource() {

		ExecutionJobVertex executionJobVertex = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			if (ejv.getJobVertex().getName().toLowerCase().contains("source")) {
				executionJobVertex = ejv;
			}
		}

		if (executionJobVertex == null) {
			executionGraph.failGlobal(new ExecutionGraphException("Could not find Source"));
		}

		List<IntermediateDataSet> producedDataSets = executionJobVertex.getJobVertex().getProducedDataSets();

		if (producedDataSets.size() != 1) {
			executionGraph.failGlobal(new ExecutionGraphException("Source has not one producing output dataset"));
		}

		return executionJobVertex;
	}

	public ExecutionJobVertex findMap() {

		ExecutionJobVertex executionJobVertex = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			if (ejv.getJobVertex().getName().toLowerCase().contains("map")) {
				executionJobVertex = ejv;
			}
		}

		if (executionJobVertex == null) {
			executionGraph.failGlobal(new ExecutionGraphException("Could not find map"));
			return null;
		}

		List<JobEdge> producedDataSets = executionJobVertex.getJobVertex().getInputs();

		if (producedDataSets.size() != 1) {
			executionGraph.failGlobal(new ExecutionGraphException("Map has not one consuming input dataset"));
		}

		return executionJobVertex;
	}

	public ExecutionJobVertex findSink() {

		ExecutionJobVertex sink = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			if (ejv.getJobVertex().getName().toLowerCase().contains("sink")) {
				sink = ejv;
			}
		}

		if (sink == null) {
			executionGraph.failGlobal(new ExecutionGraphException("Could not find map"));
			return null;
		} else {
			return sink;
		}
	}

	private ExecutionJobVertex buildFilterExecutionJobVertex(ExecutionJobVertex source, ExecutionJobVertex map) {

		String operatorName = "IntroducedFilterOperator";

		JobVertex filterJobVertex = new JobVertex(operatorName, new JobVertexID());
		filterJobVertex.setParallelism(map.getParallelism());
		filterJobVertex.setInvokableClass(OneInputStreamTask.class);

		LOG.info("Creating new operator '{}' with parallelism {}", operatorName, map.getParallelism());

		IntermediateDataSet filterProducingDataset = new IntermediateDataSet(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			filterJobVertex);

		LOG.info("Created intermediateDataSet with id {}", filterProducingDataset.getId());

		filterJobVertex.createAndAddResultDataSet(filterProducingDataset.getId(), ResultPartitionType.PIPELINED);

		List<IntermediateDataSet> producedDataSets = source.getJobVertex().getProducedDataSets();

		if (producedDataSets.size() != 1) {
			executionGraph.failGlobal(new IllegalStateException("Source has more than one producing dataset"));
			throw new IllegalStateException("Source has more than one producing dataset");
		}

		IntermediateDataSet sourceProducedDataset = producedDataSets.get(0);

		// Connect source IDS as input for FilterOperator
		filterJobVertex.connectDataSetAsInput(sourceProducedDataset, DistributionPattern.ALL_TO_ALL);

		try {
			ExecutionJobVertex vertex =
				new ExecutionJobVertex(executionGraph,
					filterJobVertex,
					map.getParallelism(),
					rpcCallTimeout,
					executionGraph.getGlobalModVersion(),
					System.currentTimeMillis());

			vertex.connectToPredecessorsRuntime(executionGraph.getIntermediateResults());

			ExecutionJobVertex previousTask = executionGraph.getTasks().putIfAbsent(filterJobVertex.getID(), vertex);
			if (previousTask != null) {
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
					filterJobVertex.getID(), vertex, previousTask));
			}

			// Add IntermediateResult to ExecutionGraph
			for (IntermediateResult res : vertex.getProducedDataSets()) {

				LOG.debug("Adding IntermediateResult {} to ExecutionGraph", res);

				IntermediateResult previousDataSet = executionGraph.getIntermediateResults().putIfAbsent(res.getId(), res);
				if (previousDataSet != null) {
					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
						res.getId(), res, previousDataSet));
				}
			}

			executionGraph.getVerticesInCreationOrder().add(vertex);
			executionGraph.addNumVertices(vertex.getParallelism());

			return vertex;
		} catch (JobException jobException) {
			executionGraph.failGlobal(jobException);
			return null;
		}
	}
}