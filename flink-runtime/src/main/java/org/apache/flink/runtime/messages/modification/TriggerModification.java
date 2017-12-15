package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.util.Set;

public class TriggerModification extends AbstractModificationMessage {

	private final long timestamp;
	private final Set<ExecutionAttemptID> vertexIds;
	private final Set<Integer> subTaskIndicesToSpill;
	private final ModificationCoordinator.ModificationAction modificationAction;
	private final long checkpointIDToModify;

	public TriggerModification(JobID job,
							   ExecutionAttemptID taskExecutionId,
							   long modificationID,
							   long timestamp,
							   Set<ExecutionAttemptID> vertexIDs,
							   Set<Integer> operatorSubTaskIndices,
							   ModificationCoordinator.ModificationAction action,
							   long checkpointIDToModify) {
		super(job, taskExecutionId, modificationID);

		this.timestamp = timestamp;
		this.vertexIds = vertexIDs;
		this.subTaskIndicesToSpill = operatorSubTaskIndices;
		this.modificationAction = action;
		this.checkpointIDToModify = checkpointIDToModify;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Set<ExecutionAttemptID> getExecutionAttempsToModify() {
		return vertexIds;
	}

	public ModificationCoordinator.ModificationAction getModificationAction() {
		return modificationAction;
	}

	public Set<Integer> getSubTaskIndicesToSpill() {
		return subTaskIndicesToSpill;
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Modification %d for (%s/%s) @ %d",
			getModificationID(), getJobID(), getTaskExecutionId(), timestamp);
	}

	public long getCheckpointIDToModify() {
		return checkpointIDToModify;
	}
}
