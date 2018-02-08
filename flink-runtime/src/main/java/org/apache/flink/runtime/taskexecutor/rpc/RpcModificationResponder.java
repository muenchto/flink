package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.streaming.runtime.modification.ModificationResponder;
import org.apache.flink.util.Preconditions;

public class RpcModificationResponder implements ModificationResponder {

	public RpcModificationResponder() {}

	@Override
	public void acknowledgeModification(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void declineModification(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void ignoreModification(JobID jobId, ExecutionAttemptID executionId, long modificationID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void acknowledgeStateMigration(JobID jobId, ExecutionAttemptID executionId, long checkpointId, CheckpointMetrics checkpointMetrics, SubtaskState subtaskState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void acknowledgeSpillingForNewOperator(JobID jobId, ExecutionAttemptID executionId, long modificationID) {

	}
}
