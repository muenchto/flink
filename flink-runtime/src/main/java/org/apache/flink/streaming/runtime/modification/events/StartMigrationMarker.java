package org.apache.flink.streaming.runtime.modification.events;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StartMigrationMarker extends RuntimeEvent {

	private final long modificationID;
	private final long timestamp;
	private final Map<ExecutionAttemptID, Set<Integer>> spillingVertices; // Mutually exclusive, either one or the other
	private final Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices;
	private final Set<ExecutionAttemptID> notPausingOperators;
	private final long checkpointIDToModify;

	public StartMigrationMarker(long modificationID,
								long timestamp,
								Map<ExecutionAttemptID, Set<Integer>> spillingVertices,
								Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices,
								Set<ExecutionAttemptID> notPausingOperators,
								long checkpointIDToModify) {
		this.modificationID = modificationID;
		this.timestamp = timestamp;
		this.spillingVertices = spillingVertices;
		this.stoppingVertices = stoppingVertices;
		this.notPausingOperators = notPausingOperators;
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

	public Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> getStoppingVertices() {
		return stoppingVertices;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		StartMigrationMarker that = (StartMigrationMarker) o;
		return modificationID == that.modificationID &&
			timestamp == that.timestamp &&
			checkpointIDToModify == that.checkpointIDToModify &&
			Objects.equals(spillingVertices, that.spillingVertices) &&
			Objects.equals(stoppingVertices, that.stoppingVertices) &&
			Objects.equals(notPausingOperators, that.notPausingOperators);
	}

	@Override
	public int hashCode() {
		return Objects.hash(modificationID, timestamp, spillingVertices, stoppingVertices, notPausingOperators, checkpointIDToModify);
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

	public Set<ExecutionAttemptID> getNotPausingOperators() {
		return notPausingOperators;
	}
}
