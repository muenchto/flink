package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class AcknowledgeModification extends AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7606214777192401493L;

	public AcknowledgeModification(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		super(job, taskExecutionId, checkpointId);
	}

	@Override
	public String toString() {
		return String.format("Confirm Task Modification %d for (%s/%s)",
			getModificationID(), getJobID(), getTaskExecutionId());
	}
}
