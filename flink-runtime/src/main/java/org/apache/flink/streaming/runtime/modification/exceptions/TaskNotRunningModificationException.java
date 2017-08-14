package org.apache.flink.streaming.runtime.modification.exceptions;

public class TaskNotRunningModificationException extends AbstractModificationException {

	private static final long serialVersionUID = 1L;

	public TaskNotRunningModificationException(String taskName) {
		super("Task " + taskName + " was not running");
	}
}
