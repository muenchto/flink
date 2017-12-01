package org.apache.flink.runtime.iterative.event;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;

/**
 * Represents, that one task wants to enter the {@link org.apache.flink.runtime.execution.ExecutionState#PAUSING}.
 * The corresponding {@link org.apache.flink.runtime.io.network.partition.ResultSubpartition} should take measures,
 * to spill all buffers to disk temporarily, until another task starts consuming again.
 */
public class PausingTaskEvent extends TaskEvent {

	private int taskIndex;

	private long upcomingCheckpointID;

	public PausingTaskEvent(int taskIndex, long upcomingCheckpointID) {
		this.taskIndex = taskIndex;
		this.upcomingCheckpointID = upcomingCheckpointID;
	}

	public int getTaskIndex() {
		return taskIndex;
	}

	public long getUpcomingCheckpointID() {
		return upcomingCheckpointID;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.taskIndex);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.taskIndex = in.readInt();
	}
}
