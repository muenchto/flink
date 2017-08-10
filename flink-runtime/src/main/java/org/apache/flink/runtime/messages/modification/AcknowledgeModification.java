package org.apache.flink.runtime.messages.modification;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.AbstractCheckpointMessage;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class AcknowledgeModification extends AbstractCheckpointMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;

	public AcknowledgeModification(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		super(job, taskExecutionId, checkpointId);
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Modification %d for (%s/%s)",
			getCheckpointId(), getJob(), getTaskExecutionId());
	}
}
