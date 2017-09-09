package org.apache.flink.streaming.runtime.modification.exceptions;

import org.apache.flink.api.common.JobID;

public class OperatorNotFoundException extends AbstractModificationException {

	private static final long serialVersionUID = 1L;

	public OperatorNotFoundException(String operatorName, JobID jobID) {
		super("Could not find Operator '" + operatorName + "' for job with id " + jobID);
	}
}
