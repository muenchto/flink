package org.apache.flink.streaming.runtime.modification;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.messages.modification.AcknowledgeModification;
import org.apache.flink.runtime.messages.modification.DeclineModification;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.modification.exceptions.OperatorNotFoundException;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ModificationCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(ModificationCoordinator.class);

	private static final long DUMMY_MODIFICATION_ID = 123;

	private final ExecutionGraph executionGraph;
	private final Time rpcCallTimeout;
	private final Collection<BlobKey> blobKeys;
	private ExecutionAttemptID stoppedMapExecutionAttemptID;
	private int parallelSubTaskIndex;

	public ModificationCoordinator(ExecutionGraph executionGraph, Time rpcCallTimeout) {
		this.executionGraph = Preconditions.checkNotNull(executionGraph);
		this.rpcCallTimeout = rpcCallTimeout;
		this.blobKeys = new HashSet<>();
	}

	public boolean receiveAcknowledgeMessage(AcknowledgeModification acknowledgeModification) {
		if (acknowledgeModification.getModificationID() == DUMMY_MODIFICATION_ID) {
			LOG.debug("Received successful acknowledge modification message");
			return true;
		} else {
			LOG.debug("Received wrong acknowledge modification message: {}", acknowledgeModification);
			return false;
		}
	}

	public String getTMDetails() {

		String details = "";

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			details += "\nAttemptID: " + executionVertex.getCurrentExecutionAttempt().getAttemptId() +
				" - TM ID: " + executionVertex.getCurrentAssignedResource().getTaskManagerID() +
				" - TM Location: " + executionVertex.getCurrentAssignedResource().getTaskManagerLocation() +
				" - Name: " + executionVertex.getTaskNameWithSubtaskIndex();
		}

		return details;
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

	public void stopMapInstance(ResourceID taskManagerID) throws OperatorNotFoundException {
		ExecutionVertex operatorInstanceToStop = getMapExecutionVertexToStop(taskManagerID);

		if (operatorInstanceToStop == null) {
			throw new OperatorNotFoundException("Map", executionGraph.getJobID(), taskManagerID);
		}

		stoppedMapExecutionAttemptID = operatorInstanceToStop.getCurrentExecutionAttempt().getAttemptId();
		parallelSubTaskIndex = operatorInstanceToStop.getCurrentExecutionAttempt().getParallelSubtaskIndex();

		operatorInstanceToStop.getCurrentExecutionAttempt().stopForMigration();
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
			stoppedExecutionVertex.resetForNewExecutionModification(System.currentTimeMillis(), executionGraph.getGlobalModVersion());

			stoppedExecutionVertex
				.getCurrentExecutionAttempt()
				.scheduleForMigration(
					executionGraph.getSlotProvider(),
					executionGraph.isQueuedSchedulingAllowed(),
					taskmanagerID);

		} catch (GlobalModVersionMismatch globalModVersionMismatch) {
			executionGraph.failGlobal(globalModVersionMismatch);
			globalModVersionMismatch.printStackTrace();
		}
	}

	public boolean receiveDeclineMessage(DeclineModification declineModification) {
		if (declineModification.getModificationID() == DUMMY_MODIFICATION_ID) {
			LOG.debug("Received successful decline modification message");
			return true;
		} else {
			LOG.debug("Received wrong decline modification message: {}", declineModification);
			return false;
		}
	}

	public void addedNewOperatorJar(Collection<BlobKey> blobKeys) {
		LOG.debug("Adding BlobKeys {} for executionGraph {}.",
			StringUtils.join(blobKeys, ","),
			executionGraph.getJobID());

		this.blobKeys.addAll(blobKeys);
	}

	public void pauseSink() {
		ExecutionJobVertex source = findSource();

		ExecutionJobVertex sink = findSink();

		List<JobVertexID> vertexIDS = Collections.singletonList(sink.getJobVertexId());

		for (ExecutionVertex executionVertex : source.getTaskVertices()) {
			Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

			currentExecutionAttempt.triggerModification(
				DUMMY_MODIFICATION_ID,
				System.currentTimeMillis(),
				vertexIDS);
		}
	}

	public void modifySinkInstance(ExecutionAttemptID newOperatorExecutionAttemptID) {
		ExecutionJobVertex sink = findSink();

		Preconditions.checkNotNull(sink);
		Preconditions.checkArgument(sink.getTaskVertices().length == 1);

		ExecutionVertex executionVertex = sink.getTaskVertices()[0];
		executionVertex.getCurrentExecutionAttempt()
			.triggerResumeWithDifferentInputs(
				rpcCallTimeout,
				newOperatorExecutionAttemptID,
				parallelSubTaskIndex);
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
		ExecutionJobVertex source = findSource();

		ExecutionJobVertex map = findFilter();

		List<JobVertexID> vertexIDS = Collections.singletonList(map.getJobVertexId());

		for (ExecutionVertex executionVertex : source.getTaskVertices()) {
			Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

			currentExecutionAttempt.triggerModification(
				DUMMY_MODIFICATION_ID,
				System.currentTimeMillis(),
				vertexIDS);
		}
	}

	public void pauseJob() {
		ExecutionJobVertex source = findSource();

		ExecutionJobVertex map = findMap();

		List<JobVertexID> vertexIDS = Collections.singletonList(map.getJobVertexId());

		for (ExecutionVertex executionVertex : source.getTaskVertices()) {
			Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

			currentExecutionAttempt.triggerModification(
				DUMMY_MODIFICATION_ID,
				System.currentTimeMillis(),
				vertexIDS);
		}
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

			// TODO Add to failover strategy

			return vertex;
		} catch (JobException jobException) {
			executionGraph.failGlobal(jobException);
			return null;
		}
	}
}
