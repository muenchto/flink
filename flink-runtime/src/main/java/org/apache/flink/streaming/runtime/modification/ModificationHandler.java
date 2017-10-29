package org.apache.flink.streaming.runtime.modification;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores information, whether a specific task has acted upon a specific modification identified
 * by the modificationId. This is necessary, as modification trigger may appear multiple times
 * with the same modification id for the same modification action.
 */
public class ModificationHandler {

	private final List<Long> pastCheckpoints = new ArrayList<>(4);

	public void handledModification(long modificationId) {
		pastCheckpoints.add(modificationId);
	}

	public List<Long> getHandledModifications() {
		return pastCheckpoints;
	}
}
