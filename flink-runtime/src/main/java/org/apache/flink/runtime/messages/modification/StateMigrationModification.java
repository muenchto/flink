package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

public class StateMigrationModification extends AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7616214777192401493L;

	private final CheckpointMetrics checkpointMetrics;

	private final SubtaskState subtaskState;

	public StateMigrationModification(JobID job,
									  ExecutionAttemptID taskExecutionId,
									  long modificationId,
									  CheckpointMetrics checkpointMetrics,
									  SubtaskState subtaskState) {
		super(job, taskExecutionId, modificationId);
		this.checkpointMetrics = checkpointMetrics;
		this.subtaskState = subtaskState;
	}

	@Override
	public String toString() {
		return String.format("State Migration Modification %d for (%s/%s)",
			getModificationID(), getJob(), getTaskExecutionId());
	}

	public SubtaskState getSubtaskState() {
		return subtaskState;
	}

	public CheckpointMetrics getCheckpointMetrics() {
		return checkpointMetrics;
	}
}
