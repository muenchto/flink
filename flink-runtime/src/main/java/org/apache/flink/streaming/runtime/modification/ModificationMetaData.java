package org.apache.flink.streaming.runtime.modification;

public class ModificationMetaData {

	private static final long serialVersionUID = -2387652345781312442L;

	/** The ID of the modification */
	private final long modificationID;

	/** The timestamp of the modification */
	private final long timestamp;

	public ModificationMetaData(long modificationID, long timestamp) {
		this.modificationID = modificationID;
		this.timestamp = timestamp;
	}

	public long getModificationID() {
		return modificationID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ModificationMetaData that = (ModificationMetaData) o;

		if (modificationID != that.modificationID) return false;
		return timestamp == that.timestamp;
	}

	@Override
	public int hashCode() {
		int result = (int) (modificationID ^ (modificationID >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "ModificationMetaData{" +
			"modificationID=" + modificationID +
			", timestamp=" + timestamp +
			'}';
	}
}
