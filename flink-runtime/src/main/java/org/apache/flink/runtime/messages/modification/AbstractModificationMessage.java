package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * The base class of all modification messages.
 */
public abstract class AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = 186780414819428178L;

	/**
	 * The job to which this message belongs
	 */
	private final JobID job;

	/**
	 * The task execution that is source/target of the checkpoint message
	 */
	private final ExecutionAttemptID taskExecutionId;

	/**
	 * The ID of the checkpoint that this message coordinates
	 */
	private final long modificationID;

	protected AbstractModificationMessage(JobID job, ExecutionAttemptID taskExecutionId, long modificationID) {
		this.job = checkNotNull(job);
		this.taskExecutionId = checkNotNull(taskExecutionId);
		this.modificationID = modificationID;
	}

	// --------------------------------------------------------------------------------------------

	public JobID getJobID() {
		return job;
	}

	public ExecutionAttemptID getTaskExecutionId() {
		return taskExecutionId;
	}

	public long getModificationID() {
		return modificationID;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AbstractModificationMessage that = (AbstractModificationMessage) o;

		if (modificationID != that.modificationID) return false;
		if (!job.equals(that.job)) return false;
		return taskExecutionId.equals(that.taskExecutionId);
	}

	@Override
	public int hashCode() {
		int result = job.hashCode();
		result = 31 * result + taskExecutionId.hashCode();
		result = 31 * result + (int) (modificationID ^ (modificationID >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "AbstractModificationMessage(" + modificationID + ':' + job + '/' + taskExecutionId + ')';
	}
}
