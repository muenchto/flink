package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

/**
 * This event marks a subpartition as spilling to disk.
 */
public class SpillToDiskMarker extends RuntimeEvent {

	/** The singleton instance of this event */
	public static final SpillToDiskMarker INSTANCE = new SpillToDiskMarker();

	// ------------------------------------------------------------------------

	// not instantiable
	private SpillToDiskMarker() {}

	// ------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) {}

	@Override
	public void write(DataOutputView out) {}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return 1965146673;
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj.getClass() == SpillToDiskMarker.class;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
