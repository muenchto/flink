package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.modification.*;
import org.apache.flink.streaming.runtime.modification.ModificationResponder;
import org.apache.flink.util.Preconditions;

public class ActorGatewayModificationResponder implements ModificationResponder {

	private final ActorGateway actorGateway;

	public ActorGatewayModificationResponder(ActorGateway actorGateway) {
		this.actorGateway = Preconditions.checkNotNull(actorGateway);
	}

	@Override
	public void acknowledgeModification(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId) {
		AcknowledgeModification message = new AcknowledgeModification(jobID, executionAttemptID, checkpointId);

		actorGateway.tell(message);
	}

	@Override
	public void declineModification(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, Throwable cause) {
		DeclineModification decline = new DeclineModification(jobID, executionAttemptID, checkpointId, cause);

		actorGateway.tell(decline);
	}

	@Override
	public void ignoreModification(JobID jobId, ExecutionAttemptID executionId, long modificationID) {
		IgnoreModification ignore = new IgnoreModification(jobId, executionId, modificationID);

		actorGateway.tell(ignore);
	}

	@Override
	public void acknowledgeStateMigration(JobID jobId, ExecutionAttemptID executionId, long checkpointId, CheckpointMetrics checkpointMetrics, SubtaskState subtaskState) {
		StateMigrationModification ignore = new StateMigrationModification(jobId, executionId, checkpointId, checkpointMetrics, subtaskState);

		actorGateway.tell(ignore);
	}

	@Override
	public void acknowledgeSpillingForNewOperator(JobID jobId, ExecutionAttemptID executionId, long modificationID) {
		AcknowledgeSpillingMessage message = new AcknowledgeSpillingMessage(jobId, executionId, modificationID);

		actorGateway.tell(message);
	}
}
