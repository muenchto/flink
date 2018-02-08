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

	/**
	 * Ignores a certain modification identified by the modification id
	 * as the modification is not relevant for this operator.
	 *
	 * @param jobId Job ID of the running job
	 * @param executionId Execution attempt ID of the running task
	 * @param modificationID The ID of the ignored modification
	 */
	void ignoreModification(JobID jobId, ExecutionAttemptID executionId, long modificationID);


	/**
	 * Acknowledges the state migration for a given pause command.
	 * @param jobId The jobID
	 * @param executionId The executionID
	 * @param checkpointId The checkpointID
	 * @param checkpointMetrics The checkpoint metrics
	 * @param subtaskState The state
	 */
	void acknowledgeStateMigration(JobID jobId, ExecutionAttemptID executionId,
								   long checkpointId,
								   CheckpointMetrics checkpointMetrics,
								   SubtaskState subtaskState);

	void acknowledgeSpillingForNewOperator(JobID jobId,
										   ExecutionAttemptID executionId,
										   long modificationID);
}
