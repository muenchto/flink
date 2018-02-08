package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TriggerMigration extends AbstractModificationMessage {

	private final long timestamp;
	private final Map<ExecutionAttemptID, Set<Integer>> spillingVertices; // Mutually exclusive, either one or the other
	private final Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices;
	private final Set<ExecutionAttemptID> notPausingOperators;
	private final long checkpointIDToModify;

	public TriggerMigration(JobID job,
							ExecutionAttemptID taskExecutionId,
							long modificationID,
							long timestamp,
							Map<ExecutionAttemptID, Set<Integer>> spillingVertices,
							Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices,
							Set<ExecutionAttemptID> notPausingOperators,
							long checkpointIDToModify) {
		super(job, taskExecutionId, modificationID);

		this.timestamp = timestamp;
		this.spillingVertices = spillingVertices;
		this.stoppingVertices = stoppingVertices;
		this.notPausingOperators = notPausingOperators;
		this.checkpointIDToModify = checkpointIDToModify;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Modification %d for (%s/%s) @ %d",
			getModificationID(), getJobID(), getTaskExecutionId(), timestamp);
	}

	public long getCheckpointIDToModify() {
		return checkpointIDToModify;
	}

	public Map<ExecutionAttemptID, Set<Integer>> getSpillingVertices() {
		return spillingVertices;
	}

	public Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> getStoppingVertices() {
		return stoppingVertices;
	}

	public Set<ExecutionAttemptID> getNotPausingOperators() {
		return notPausingOperators;
	}
}
