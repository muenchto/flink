package org.apache.flink.streaming.runtime.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.Task;

public interface ModificationResponder {

	/**
	 * Acknowledges the given modification.
	 *
	 * @param jobID
	 *             Job ID of the running job
	 * @param executionAttemptID
	 *             Execution attempt ID of the running task
	 * @param checkpointId
	 *             Meta data for this modification
	 */
	void acknowledgeModification(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId);

	/**
	 * Declines the given modification.
	 *
	 * @param jobID Job ID of the running job
	 * @param executionAttemptID Execution attempt ID of the running task
	 * @param checkpointId The ID of the declined modification
	 * @param cause The optional cause why the modification was declined
	 */
	void declineModification(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		Throwable cause);
}
