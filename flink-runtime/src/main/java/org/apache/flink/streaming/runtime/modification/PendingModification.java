package org.apache.flink.streaming.runtime.modification;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.*;

/**
 * A pending modification is a modification that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it.
 */
public class PendingModification {

	/**
	 * Result of the {@link PendingModification#acknowledgedTasks} method.
	 */
	public enum TaskModificationAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}

	/**
	 * The PendingCheckpoint logs to the same logger as the ModificationCoordinator
	 */
	private static final Logger LOG = LoggerFactory.getLogger(ModificationCoordinator.class);

	private final Object lock = new Object();

	private final JobID jobId;

	private final long modificationId;

	private final long modificationTimestamp;

	private final String modificationDescription;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/**
	 * The promise to fulfill once the checkpoint has been completed.
	 */
	private final FlinkCompletableFuture<Boolean> onCompletionPromise;

	private int numAcknowledgedTasks;

	private volatile ScheduledFuture<?> cancellerHandle;

	private boolean discarded;

	// --------------------------------------------------------------------------------------------

	public PendingModification(
		JobID jobId,
		long modificationId,
		long modificationTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		String modificationDescription) {

		checkArgument(verticesToConfirm.size() > 0,
			"Modification needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.modificationId = modificationId;
		this.modificationTimestamp = modificationTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.modificationDescription = checkNotNull(modificationDescription);

		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = new FlinkCompletableFuture<>();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public long getModificationId() {
		return modificationId;
	}

	public long getModificationTimestamp() {
		return modificationTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public boolean isFullyAcknowledged() {
		return this.notYetAcknowledgedTasks.isEmpty();
	}

	public String getModificationDescription() {
		return modificationDescription;
	}

	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Sets the handle for the canceller to this pending checkpoint. This method fails
	 * with an exception if a handle has already been set.
	 */
	public void setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
		synchronized (lock) {
			if (this.cancellerHandle == null) {
				this.cancellerHandle = cancellerHandle;
			} else {
				throw new IllegalStateException("A canceller handle was already set");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	/**
	 * Returns the completion future.
	 *
	 * @return A future to the completed checkpoint
	 */
	public Future<Boolean> getCompletionFuture() {
		return onCompletionPromise;
	}

	public CompletedModification finalizeCheckpoint() {
		synchronized (lock) {
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				onCompletionPromise.complete(true);

				return new CompletedModification(
					jobId,
					modificationId,
					modificationTimestamp,
					acknowledgedTasks,
					modificationDescription);

			} catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrow(t);
				return null;
			}
		}
	}

	/**
	 * Acknowledges the task with the given execution attempt id and the given subtask state.
	 *
	 * @param executionAttemptId of the acknowledged task
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskModificationAcknowledgeResult acknowledgeTask(ExecutionAttemptID executionAttemptId) {

		synchronized (lock) {

			if (discarded) {
				return TaskModificationAcknowledgeResult.DISCARDED;
			}

			final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

			if (vertex == null) {
				if (acknowledgedTasks.contains(executionAttemptId)) {
					return TaskModificationAcknowledgeResult.DUPLICATE;
				} else {
					return TaskModificationAcknowledgeResult.UNKNOWN;
				}
			} else {
				acknowledgedTasks.add(executionAttemptId);
			}

			++numAcknowledgedTasks;

			return TaskModificationAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Aborts a checkpoint because it expired (took too long).
	 */
	public void abortExpired() {
		discarded = true;
		Exception cause = new Exception("Checkpoint expired before completing");
		onCompletionPromise.completeExceptionally(cause);
		reportFailedCheckpoint(cause);
	}

	public void abortDeclined() {
		discarded = true;
		Exception cause = new Exception("Checkpoint was declined (tasks not ready)");
		onCompletionPromise.completeExceptionally(cause);
		reportFailedCheckpoint(cause);
	}

	public void abortError(Throwable cause) {
		discarded = true;
		Exception failure = new Exception("Checkpoint failed: " + cause.getMessage(), cause);
		onCompletionPromise.completeExceptionally(failure);
		reportFailedCheckpoint(failure);
	}

	/**
	 * Reports a failed checkpoint with the given optional cause.
	 *
	 * @param cause The failure cause or <code>null</code>.
	 */
	private void reportFailedCheckpoint(Exception cause) {
		LOG.error("Failed modification at {} due to: {}", System.currentTimeMillis(), cause);
	}

	@Override
	public String toString() {
		return String.format("Pending Modification %d @ %d - confirmed=%d, pending=%d",
			modificationId, modificationTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}

