package org.apache.flink.streaming.runtime.modification.events;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StartMigrationMarker extends RuntimeEvent {

	private final long modificationID;
	private final long timestamp;
	private final Map<ExecutionAttemptID, Set<Integer>> spillingVertices; // Mutually exclusive, either one or the other
	private final Map<ExecutionAttemptID, TaskManagerLocation> stoppingVertices;
	private final long checkpointIDToModify;

	public StartMigrationMarker(long modificationID,
								long timestamp,
								Map<ExecutionAttemptID, Set<Integer>> spillingVertices,
								Map<ExecutionAttemptID, TaskManagerLocation> stoppingVertices,
								long checkpointIDToModify) {
		this.modificationID = modificationID;
		this.timestamp = timestamp;
		this.spillingVertices = spillingVertices;
		this.stoppingVertices = stoppingVertices;

		this.checkpointIDToModify = checkpointIDToModify;
	}

	public long getModificationID() {
		return modificationID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getCheckpointIDToModify() {
		return checkpointIDToModify;
	}

	public Map<ExecutionAttemptID, Set<Integer>> getSpillingVertices() {
		return spillingVertices;
	}

	public Map<ExecutionAttemptID, TaskManagerLocation> getStoppingVertices() {
		return stoppingVertices;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof StartMigrationMarker)) return false;

		StartMigrationMarker marker = (StartMigrationMarker) o;

		if (modificationID != marker.modificationID) return false;
		if (timestamp != marker.timestamp) return false;
		if (checkpointIDToModify != marker.checkpointIDToModify) return false;
		if (spillingVertices != null ? !spillingVertices.equals(marker.spillingVertices) : marker.spillingVertices != null)
			return false;
		return stoppingVertices != null ? stoppingVertices.equals(marker.stoppingVertices) : marker.stoppingVertices == null;
	}

	@Override
	public int hashCode() {
		int result = (int) (modificationID ^ (modificationID >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + (spillingVertices != null ? spillingVertices.hashCode() : 0);
		result = 31 * result + (stoppingVertices != null ? stoppingVertices.hashCode() : 0);
		result = 31 * result + (int) (checkpointIDToModify ^ (checkpointIDToModify >>> 32));
		return result;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	//
	//  These methods are inherited form the generic serialization of AbstractEvent
	//  but would require the CheckpointBarrier to be mutable. Since all serialization
	//  for events goes through the EventSerializer class, which has special serialization
	//  for the CheckpointBarrier, we don't need these methods
	//

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new UnsupportedOperationException("This method should never be called");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("This method should never be called");
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("StartMigrationMarker %d with spilling: %s and stopping: %s @ %d",
			modificationID,
			StringUtils.join(spillingVertices, ","),
			StringUtils.join(stoppingVertices, ","),
			timestamp);
	}
}
