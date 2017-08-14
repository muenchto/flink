package org.apache.flink.streaming.runtime.modification.exceptions;

public class TaskNotStatefulModificationException extends AbstractModificationException {

	private static final long serialVersionUID = 1L;

	public TaskNotStatefulModificationException(String taskName) {
		super("Task '" + taskName + "'does not support checkpointing");
	}
}
