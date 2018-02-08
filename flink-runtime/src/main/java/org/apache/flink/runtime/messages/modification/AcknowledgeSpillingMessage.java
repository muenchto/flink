package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

public class AcknowledgeSpillingMessage extends AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7616214777192401493L;

	public AcknowledgeSpillingMessage(JobID job,
									  ExecutionAttemptID taskExecutionId,
									  long modificationId) {
		super(job, taskExecutionId, modificationId);
	}

	@Override
	public String toString() {
		return String.format("State Migration Modification %d for (%s/%s)",
			getModificationID(), getJobID(), getTaskExecutionId());
	}
}
