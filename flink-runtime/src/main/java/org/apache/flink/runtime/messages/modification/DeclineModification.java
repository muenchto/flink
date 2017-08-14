package org.apache.flink.runtime.messages.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.decline.*;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.AbstractCheckpointMessage;
import org.apache.flink.runtime.util.SerializedThrowable;

public class DeclineModification extends AbstractModificationMessage implements java.io.Serializable {

	private static final long serialVersionUID = 2094094662279578953L;

	/**
	 * The reason why the checkpoint was declined
	 */
	private final Throwable reason;

	public DeclineModification(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId) {
		this(job, taskExecutionId, checkpointId, null);
	}

	public DeclineModification(JobID job, ExecutionAttemptID taskExecutionId, long checkpointId, Throwable reason) {
		super(job, taskExecutionId, checkpointId);

		// TODO May need to check for custom exceptions
		if (reason == null ||
			reason.getClass() == AlignmentLimitExceededException.class ||
			reason.getClass() == CheckpointDeclineOnCancellationBarrierException.class ||
			reason.getClass() == CheckpointDeclineSubsumedException.class ||
			reason.getClass() == CheckpointDeclineTaskNotCheckpointingException.class ||
			reason.getClass() == CheckpointDeclineTaskNotReadyException.class ||
			reason.getClass() == InputEndOfStreamException.class) {
			// null or known common exceptions that cannot reference any dynamically loaded code
			this.reason = reason;
		} else {
			// some other exception. replace with a serialized throwable, to be on the safe side
			this.reason = new SerializedThrowable(reason);
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the reason why the checkpoint was declined.
	 *
	 * @return The reason why the checkpoint was declined
	 */
	public Throwable getReason() {
		return reason;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Declined Modification %d for (%s/%s): %s",
			getModificationID(), getJob(), getTaskExecutionId(), reason);
	}
}
