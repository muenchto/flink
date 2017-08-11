package org.apache.flink.streaming.runtime.modification;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ModificationCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(ModificationCoordinator.class);

	private final ExecutionGraph executionGraph;
	private final Time rpcCallTimeout;

	public ModificationCoordinator(ExecutionGraph executionGraph, Time rpcCallTimeout) {
		this.executionGraph = Preconditions.checkNotNull(executionGraph);
		this.rpcCallTimeout = rpcCallTimeout;
	}

	public void pauseMapOperator() {

		ExecutionJobVertex source = findMap();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.pauseTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void pauseMapOperatorLegacy() {
		ExecutionJobVertex source = findMap();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.pauseTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void resumeMapOperator() {
		ExecutionJobVertex source = findMap();

		ExecutionVertex[] taskVertices = source.getTaskVertices();

		for (ExecutionVertex vertex : taskVertices) {
			Execution execution = vertex.getCurrentExecutionAttempt();

			execution.getAssignedResource()
				.getTaskManagerGateway()
				.resumeTask(execution.getAttemptId(), rpcCallTimeout);
		}
	}

	public void startFilterOperator() {

		ExecutionJobVertex source = findMap();

		ExecutionVertex[] sourceTaskVertices = source.getTaskVertices();

		if (sourceTaskVertices.length != 1) {
			executionGraph.failGlobal(new RuntimeException("Failed to find sourceTaskVertex"));
		}

		Execution execution = sourceTaskVertices[0].getCurrentExecutionAttempt();

		ExecutionJobVertex executionJobVertex = buildFilterExecution();

		boolean b = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt()
			.scheduleForRuntimeExecution(
				executionGraph.getSlotProvider(),
				executionGraph.isQueuedSchedulingAllowed(),
				execution.getAttemptId());


//		for (ExecutionVertex vertex : sourceTaskVertices) {
//			Execution execution = vertex.getCurrentExecutionAttempt();
//
//			execution.getVertex().createDeploymentDescriptor();
//
//			execution.getAssignedResource()
//				.getTaskManagerGateway()
//				.introduceNewOperator(execution.getAttemptId(), slot.getAllocatedSlot().getSlotAllocationId(), slot.getRoot().getSlotNumber(), rpcCallTimeout);
//		}
	}


	public String getDetails() throws JobException {

		StringBuilder currentPlan = new StringBuilder();

		currentPlan.append(executionGraph.getJsonPlan()).append("\n");

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			currentPlan.append(ejv.generateDebugString()).append("\n");
		}

		JobInformation jobInformation = executionGraph.getJobInformation();

		currentPlan.append("JobInfo: ").append(jobInformation).append(jobInformation.getJobConfiguration()).append("\n");

		for (Map.Entry<JobVertexID, ExecutionJobVertex> vertex: executionGraph.getTasks().entrySet()) {
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

			for (IntermediateResult result:vertex.getValue().getInputs()) {
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

		for (Map.Entry<ExecutionAttemptID, Execution> exec: executionGraph.getRegisteredExecutions().entrySet()) {
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

	private ExecutionJobVertex findSource() {

		ExecutionJobVertex executionJobVertex = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			if (ejv.getJobVertex().getName().toLowerCase().contains("source")) {
				executionJobVertex =  ejv;
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

	private ExecutionJobVertex findMap() {

		ExecutionJobVertex executionJobVertex = null;

		for (ExecutionJobVertex ejv : executionGraph.getVerticesInCreationOrder()) {
			if (ejv.getJobVertex().getName().toLowerCase().contains("map")) {
				executionJobVertex =  ejv;
			}
		}

		if (executionJobVertex == null) {
			executionGraph.failGlobal(new ExecutionGraphException("Could not find map"));
		}

		List<JobEdge> producedDataSets = executionJobVertex.getJobVertex().getInputs();

		if (producedDataSets.size() != 1) {
			executionGraph.failGlobal(new ExecutionGraphException("Map has not one consuming input dataset"));
		}

		return executionJobVertex;
	}

	private ExecutionJobVertex buildFilterExecution() {

		JobVertex jobVertex = new JobVertex("IntroducedFilterOperator", new JobVertexID());
		jobVertex.setParallelism(1);
		jobVertex.setInvokableClass(OneInputStreamTask.class);

		IntermediateDataSet intermediateDataSet = new IntermediateDataSet(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			jobVertex);

		LOG.info("Created intermediateDataSet with id: " + intermediateDataSet.getId());

		jobVertex.createAndAddResultDataSet(intermediateDataSet.getId(), ResultPartitionType.PIPELINED);

		ExecutionJobVertex source = findSource();

		IntermediateDataSet producedDataSets = source.getJobVertex().getProducedDataSets().get(0);
		jobVertex.connectDataSetAsInput(producedDataSets, DistributionPattern.ALL_TO_ALL);

		try {
			ExecutionJobVertex vertex =
				new ExecutionJobVertex(executionGraph,
					jobVertex,
					1,
					rpcCallTimeout,
					executionGraph.getGlobalModVersion(),
					System.currentTimeMillis());

			vertex.connectToPredecessorsRuntime(executionGraph.getIntermediateResults());

			// TODO Add produced datasets data sets to intermediateResults
//			for (IntermediateResult res : ejv.getProducedDataSets()) {
//				IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
//				if (previousDataSet != null) {
//					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
//						res.getId(), res, previousDataSet));
//				}
//			}

			ExecutionJobVertex previousTask = executionGraph.getTasks().putIfAbsent(jobVertex.getID(), vertex);
			if (previousTask != null) {
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
					jobVertex.getID(), vertex, previousTask));
			}

			executionGraph.getTotalNumberOfVertices();

			executionGraph.getVerticesInCreationOrder().add(vertex);
			executionGraph.addNumVertices(vertex.getParallelism());

			// TODO Add to failover strategy

			return vertex;
		} catch (JobException e) {
			executionGraph.failGlobal(e);
		}

		return null;
	}
}
