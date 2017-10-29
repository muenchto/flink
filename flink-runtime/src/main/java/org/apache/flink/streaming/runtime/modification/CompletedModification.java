package org.apache.flink.streaming.runtime.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A completed modification.
 */
public class CompletedModification {

	private final JobID jobId;

	private final long modificationId;

	private final long modificationTimestamp;

	private final String modificationDescription;

	private final Set<ExecutionAttemptID> acknowledgedTasks;

	private final long duration;

	public CompletedModification(
		JobID jobId,
		long modificationId,
		long modificationTimestamp,
		Set<ExecutionAttemptID> acknowledgedTasks,
		String modificationDescription) {

		checkArgument(acknowledgedTasks.size() > 0,
			"Modification needs at least one vertex that has acknowledged the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.modificationId = modificationId;
		this.modificationTimestamp = modificationTimestamp;
		this.acknowledgedTasks = checkNotNull(acknowledgedTasks);
		this.modificationDescription = checkNotNull(modificationDescription);
		this.duration = System.currentTimeMillis() - modificationTimestamp;
	}

	public JobID getJobId() {
		return jobId;
	}

	public long getModificationId() {
		return modificationId;
	}

	public long getModificationTimestamp() {
		return modificationTimestamp;
	}

	public int getNumberOfAcknowledgedTasks() {
		return acknowledgedTasks.size();
	}

	public Set<ExecutionAttemptID> getAcknowledgedTasks() {
		return acknowledgedTasks;
	}

	public String getModificationDescription() {
		return modificationDescription;
	}

	public long getDuration() {
		return duration;
	}

	@Override
	public String toString() {
		return String.format("Completed Modification %d @ %d - confirmed=%d",
			modificationId, modificationTimestamp, getNumberOfAcknowledgedTasks());
	}
}

