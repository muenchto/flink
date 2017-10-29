package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

public class IgnoreModification extends AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = -7616214777192401493L;

	public IgnoreModification(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		super(job, taskExecutionId, checkpointId);
	}

	@Override
	public String toString() {
		return String.format("Ignore Task Modification %d for (%s/%s)",
			getModificationID(), getJob(), getTaskExecutionId());
	}
}
