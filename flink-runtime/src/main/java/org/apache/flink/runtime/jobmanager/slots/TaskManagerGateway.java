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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Task manager gateway interface to communicate with the task manager.
 */
public interface TaskManagerGateway {

	/**
	 * Return the address of the task manager with which the gateway is associated.
	 *
	 * @return Address of the task manager with which this gateway is associated.
	 */
	String getAddress();

	/**
	 * Disconnect the task manager from the job manager.
	 *
	 * @param instanceId identifying the task manager
	 * @param cause of the disconnection
	 */
	void disconnectFromJobManager(InstanceID instanceId, Exception cause);

	/**
	 * Stop the cluster.
	 *
	 * @param applicationStatus to stop the cluster with
	 * @param message to deliver
	 */
	void stopCluster(final ApplicationStatus applicationStatus, final String message);

	/**
	 * Request the stack trace from the task manager.
	 *
	 * @param timeout for the stack trace request
	 * @return Future for a stack trace
	 */
	Future<StackTrace> requestStackTrace(final Time timeout);

	/**
	 * Request a stack trace sample from the given task.
	 *
	 * @param executionAttemptID identifying the task to sample
	 * @param sampleId of the sample
	 * @param numSamples to take from the given task
	 * @param delayBetweenSamples to wait for
	 * @param maxStackTraceDepth of the returned sample
	 * @param timeout of the request
	 * @return Future of stack trace sample response
	 */
	Future<StackTraceSampleResponse> requestStackTraceSample(
		final ExecutionAttemptID executionAttemptID,
		final int sampleId,
		final int numSamples,
		final Time delayBetweenSamples,
		final int maxStackTraceDepth,
		final Time timeout);

	/**
	 * Submit a task to the task manager.
	 *
	 * @param tdd describing the task to submit
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	Future<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		Time timeout);

	/**
	 * Stop the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully stopped
	 */
	Future<Acknowledge> stopTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	Future<Acknowledge> cancelTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Pauses the given task
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully paused
	 */
	Future<Acknowledge> pauseTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout
	);

	/**
	 * Resumes the given task
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation  @return Future acknowledge if the task is successfully resumed
	 */
	Future<Acknowledge> resumeTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout
	);

	/**
	 * Introduces a new {@link org.apache.flink.api.common.functions.FilterFunction} to the job.
	 *
	 * @param successorExecutionAttemptID AttemptID of the successor operator, whose input needs to be redirected.
	 * @param descriptor The TaskDeploymentDescriptor for the new operator.
	 * @param operatorClassName The class name of the operator, that should be instantiated
	 * @param timeout of the submit operation  @return Future acknowledge if the operator was successfully introduced
	 */
	Future<Acknowledge> introduceNewOperator(ExecutionAttemptID successorExecutionAttemptID,
											 TaskDeploymentDescriptor descriptor,
											 Time timeout);

	/**
	 * Stop the given task for migration.
	 *
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully stopped
	 */
	Future<Acknowledge> startTaskFromMigration(TaskDeploymentDescriptor deployment, Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	Future<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		Time timeout);

	/**
	 * Fail all intermediate result partitions of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 */
	void failPartition(ExecutionAttemptID executionAttemptID);

	/**
	 * Notify the given task about a completed checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	void notifyCheckpointComplete(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp);

	/**
	 * Trigger for the given task a checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 */
	void triggerCheckpoint(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions);

	/**
	 * Trigger for the source tasks, that is should send out
	 * {@link org.apache.flink.streaming.runtime.modification.events.StartModificationMarker}
	 * to pause the job.
	 * @param attemptId identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param modificationID of the modification to trigger
	 * @param timestamp of the modification to trigger
	 * @param ids The vertexIDs, that should be paused
	 * @param operatorSubTaskIndices
	 * @param action
	 * @param checkpointIDToModify
	 */
	void triggerMigration(ExecutionAttemptID attemptId,
						  JobID jobId,
						  long modificationID,
						  long timestamp,
						  Set<ExecutionAttemptID> ids,
						  Set<Integer> operatorSubTaskIndices,
						  ModificationCoordinator.ModificationAction action,
						  long checkpointIDToModify);


	void triggerMigration(ExecutionAttemptID attemptId,
						  JobID jobId,
						  long modificationId,
						  long timestamp,
						  Map<ExecutionAttemptID, Set<Integer>> spillingToDiskIDs,
						  Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> pausingIDs,
						  Set<ExecutionAttemptID> notPausingOperators,
						  long checkpointIDToModify);

	/**
	 * Request the task manager log from the task manager.
	 *
	 * @param timeout for the request
	 * @return Future blob key under which the task manager log has been stored
	 */
	Future<BlobKey> requestTaskManagerLog(final Time timeout);

	/**
	 * Request the task manager stdout from the task manager.
	 *
	 * @param timeout for the request
	 * @return Future blob key under which the task manager stdout file has been stored
	 */
	Future<BlobKey> requestTaskManagerStdout(final Time timeout);

	FlinkFuture<Acknowledge> triggerResumeWithDifferentInputs(Time timeout,
															  ExecutionAttemptID currentSinkAttempt,
															  int stoppedMapSubTaskIndex, List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptor);

	FlinkFuture<Acknowledge>  triggerResumeWithIncreaseDoP(Time timeout,
														   ExecutionAttemptID currentSinkAttempt,
														   ExecutionAttemptID newOperatorExecutionAttemptID,
														   IntermediateResultPartitionID irpidOfThirdFilterOperator, TaskManagerLocation tmLocation,
														   int connectionIndex, int subTaskIndex);

	void addNewConsumer(ExecutionAttemptID attemptId, JobID jobId);

	FlinkFuture<Acknowledge> triggerResumeWithNewInputs(Time rpcCallTimeout,
														ExecutionAttemptID attemptId,
														List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptor);

	void switchFunction(ExecutionAttemptID attemptId, JobID jobId, BlobKey blobKey, String className, long modificationId);

	Future<Acknowledge> submitNewOperator(TaskDeploymentDescriptor deployment, String className, BlobKey key, Time timeout);
}
