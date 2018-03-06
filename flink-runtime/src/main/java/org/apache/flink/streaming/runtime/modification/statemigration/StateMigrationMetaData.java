package org.apache.flink.streaming.runtime.modification.statemigration;

import java.io.Serializable;

/**
 * Encapsulates all the meta data for a state migration.
 */
public class StateMigrationMetaData implements Serializable {

	private static final long serialVersionUID = -2387652345781312442L;

	/** The ID of the state migration */
	private final long stateMigrationId;

	private final long checkpointID;
	/** The timestamp of the state migration */
	private final long timestamp;

	public StateMigrationMetaData(long stateMigrationId, long checkpointID, long timestamp) {
		this.stateMigrationId = stateMigrationId;
		this.checkpointID = checkpointID;
		this.timestamp = timestamp;
	}

	public long getStateMigrationId() {
		return stateMigrationId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StateMigrationMetaData that = (StateMigrationMetaData) o;

		return (stateMigrationId == that.stateMigrationId) && (timestamp == that.timestamp);
	}

	@Override
	public int hashCode() {
		int result = (int) (stateMigrationId ^ (stateMigrationId >>> 32));
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "StateMigrationMetaData{" +
			"stateMigrationId=" + stateMigrationId +
			", timestamp=" + timestamp +
			'}';
	}

	public long getCheckpointID() {
		return checkpointID;
	}
}
