package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.util.List;
import java.util.Set;

public class TriggerModification extends AbstractModificationMessage {

	private final long timestamp;
	private final Set<ExecutionAttemptID> vertexIds;
	private final ModificationCoordinator.ModificationAction modificationAction;

	public TriggerModification(JobID job,
							   ExecutionAttemptID taskExecutionId,
							   long modificationID,
							   long timestamp,
							   Set<ExecutionAttemptID> vertexIDs,
							   ModificationCoordinator.ModificationAction action) {
		super(job, taskExecutionId, modificationID);

		this.timestamp = timestamp;
		this.vertexIds = vertexIDs;
		this.modificationAction = action;
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

	@Override
	public String toString() {
		return String.format("Confirm Task Modification %d for (%s/%s) @ %d",
			getModificationID(), getJobID(), getTaskExecutionId(), timestamp);
	}
}
