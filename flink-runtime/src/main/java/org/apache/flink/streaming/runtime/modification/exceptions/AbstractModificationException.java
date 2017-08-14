package org.apache.flink.streaming.runtime.modification.exceptions;


/**
 * Base class of all exceptions that indicate a declined checkpoint.
 */
public abstract class AbstractModificationException extends Exception {

	private static final long serialVersionUID = 1L;

	public AbstractModificationException(String message) {
		super(message);
	}

	public AbstractModificationException(String message, Throwable cause) {
		super(message, cause);
	}
}
