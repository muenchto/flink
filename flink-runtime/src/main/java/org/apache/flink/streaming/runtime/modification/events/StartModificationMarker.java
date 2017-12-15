package org.apache.flink.streaming.runtime.modification.events;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.io.IOException;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Indicates, that operators for all JobVertices contained in {@link #vertexIds} should pause and
 * wait for manual triggering of each {@link org.apache.flink.runtime.taskmanager.Task} to resume execution.
 */
public class StartModificationMarker extends RuntimeEvent {

	private final long modificationID;
	private final long timestamp;
	private final Set<ExecutionAttemptID> vertexIds;
	private final Set<Integer> subTasksToPause;
	private final ModificationCoordinator.ModificationAction modificationAction;

	public StartModificationMarker(long modificationID,
								   long timestamp,
								   Set<ExecutionAttemptID> vertexIDs,
								   Set<Integer> subTasksToPause,
								   ModificationCoordinator.ModificationAction action) {
		this.modificationID = modificationID;
		this.timestamp = timestamp;
		this.vertexIds = checkNotNull(vertexIDs);
		this.subTasksToPause = checkNotNull(subTasksToPause);
		this.modificationAction = checkNotNull(action);
	}

	public long getModificationID() {
		return modificationID;
	}

	public Set<ExecutionAttemptID> getJobVertexIDs() {
		return vertexIds;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public ModificationCoordinator.ModificationAction getModificationAction() {
		return modificationAction;
	}

	public Set<Integer> getSubTasksToPause() {
		return subTasksToPause;
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
		return String.format("StartModificationMarker %d with ids: %s @ %d",
			modificationID, StringUtils.join(vertexIds, ","), timestamp);
	}
}
