package org.apache.flink.streaming.runtime.modification.events;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents, that the current modifications has been finished and execution of the graph may continue.
 */
public class CancelModificationMarker extends RuntimeEvent {

	private final long modificationID;
	private final long timestamp;
	private final List<JobVertexID> vertexIds;

	public CancelModificationMarker(long modificationID, long timestamp, List<JobVertexID> vertexIDs) {
		this.modificationID = modificationID;
		this.timestamp = timestamp;
		this.vertexIds = checkNotNull(vertexIDs);
	}

	public long getModificationID() {
		return modificationID;
	}

	public List<JobVertexID> getJobVertexIDs() {
		return vertexIds;
	}

	public long getTimestamp() {
		return timestamp;
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
		return String.format("CancelModificationMarker with ids: %d @ %s - %d",
			modificationID, StringUtils.join(vertexIds, ","), timestamp);
	}
}
