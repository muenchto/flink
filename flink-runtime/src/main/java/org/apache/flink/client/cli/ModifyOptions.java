/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.*;

/**
 * Command line options for the MODIFY command
 */
public class ModifyOptions extends CommandLineOptions {

	private final String[] args;
	private final String command;
	private final String jarFile;
	private final String operatorClassName;
	private final String jobID;

	public ModifyOptions(CommandLine line) {
		super(line);
		this.args = line.getArgs();
		this.command = line.hasOption(MODIFY_COMMAND_OPTION.getOpt()) ?
			line.getOptionValue(MODIFY_COMMAND_OPTION.getOpt()) : null;
		this.operatorClassName = line.hasOption(CLASS_OPTION.getOpt()) ?
			line.getOptionValue(CLASS_OPTION.getOpt()) : null;
		this.jobID = line.hasOption(JOB_COMMAND_OPTION.getOpt()) ?
			line.getOptionValue(JOB_COMMAND_OPTION.getOpt()) : null;
		this.jarFile = line.getOptionValue(JAR_OPTION.getOpt());
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public String getCommand() {
		return command;
	}

	public String getJarFile() {
		return jarFile;
	}

	public String getOperatorClassName() {
		return operatorClassName;
	}

	public String getJobID() {
		return jobID;
	}
}
