
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

package org.apache.flink.runtime.io.network.api.serialization;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions.CheckpointType;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputChannelDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionLocation;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.api.*;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.iterative.event.PausingTaskEvent;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.runtime.modification.ModificationCoordinator;
import org.apache.flink.streaming.runtime.modification.events.CancelModificationMarker;
import org.apache.flink.streaming.runtime.modification.events.StartMigrationMarker;
import org.apache.flink.streaming.runtime.modification.events.StartModificationMarker;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.apache.flink.util.Preconditions;

/**
 * Utility class to serialize and deserialize task events.
 */
public class EventSerializer {

	private static final Charset STRING_CODING_CHARSET = Charset.forName("UTF-8");

	private static final int END_OF_PARTITION_EVENT = 0;

	private static final int CHECKPOINT_BARRIER_EVENT = 1;

	private static final int END_OF_SUPERSTEP_EVENT = 2;

	private static final int OTHER_EVENT = 3;

	private static final int CANCEL_CHECKPOINT_MARKER_EVENT = 4;

	private static final int MODIFICATION_START_EVENT = 5;

	private static final int MODIFICATION_CANCEL_EVENT = 6;

	private static final int SPILL_TO_DISK_MARKER = 7;

	private static final int PAUSING_TASK_EVENT = 8;

	private static final int PAUSED_OPERATOR_EVENT = 9;

	private static final int MIGRATION_START_EVENT = 10;

	// ------------------------------------------------------------------------

	public static ByteBuffer toSerializedEvent(AbstractEvent event) throws IOException {
		final Class<?> eventClass = event.getClass();
		if (eventClass == EndOfPartitionEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_PARTITION_EVENT });
		} else if (eventClass == SpillToDiskMarker.class) {

			SpillToDiskMarker marker = (SpillToDiskMarker) event;

			ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putInt(0, SPILL_TO_DISK_MARKER);
			buf.putInt(4, marker.getAction().ordinal());

			return buf;
		} else if (eventClass == PausingOperatorMarker.class) {

			PausingOperatorMarker marker = (PausingOperatorMarker) event;

			boolean containsLocation = marker.getDescriptor() != null;
			InputChannelDeploymentDescriptor descriptor = marker.getDescriptor();

			int bufferSize = 5;
			if (descriptor != null) {
				bufferSize += 33;

				if (descriptor.getConsumedPartitionLocation().isRemote()) {
					bufferSize += 12;

					bufferSize += descriptor.getConsumedPartitionLocation().getConnectionId().getAddress()
						.getHostName().getBytes().length;
				}
			}

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(PAUSED_OPERATOR_EVENT);
			buf.put((byte) (containsLocation ? 1 : 0));

			if (descriptor != null) {

				ResultPartitionID partitionId = descriptor.getConsumedPartitionId();

				buf.putLong(partitionId.getPartitionId().getLowerPart());
				buf.putLong(partitionId.getPartitionId().getUpperPart());

				buf.putLong(partitionId.getProducerId().getLowerPart());
				buf.putLong(partitionId.getProducerId().getUpperPart());

				ResultPartitionLocation location = descriptor.getConsumedPartitionLocation();
				buf.put((byte) (location.isLocal() ? 1 : 0));

				if (location.isRemote()) {
					InetSocketAddress inetSocketAddress = location.getConnectionId().getAddress();

					byte[] hostName = inetSocketAddress.getHostName().getBytes();
					int hostNameSize = hostName.length;

					buf.putInt(hostNameSize);
					for (byte addres : hostName) {
						buf.put(addres);
					}

					buf.putInt(inetSocketAddress.getPort());

					buf.putInt(location.getConnectionId().getConnectionIndex());
				}
			}

			buf.position(0);

			return buf;

		} else if (eventClass == PausingTaskEvent.class) {

			PausingTaskEvent marker = (PausingTaskEvent) event;

			ByteBuffer buf = ByteBuffer.allocate(16);
			buf.putInt(0, PAUSING_TASK_EVENT);
			buf.putInt(4, marker.getTaskIndex());
			buf.putLong(8, marker.getUpcomingCheckpointID());
			return buf;

		} else if (eventClass == CheckpointBarrier.class) {
			CheckpointBarrier barrier = (CheckpointBarrier) event;

			CheckpointOptions checkpointOptions = barrier.getCheckpointOptions();
			CheckpointType checkpointType = checkpointOptions.getCheckpointType();

			ByteBuffer buf;
			if (checkpointType == CheckpointType.FULL_CHECKPOINT) {
				buf = ByteBuffer.allocate(24);
				buf.putInt(0, CHECKPOINT_BARRIER_EVENT);
				buf.putLong(4, barrier.getId());
				buf.putLong(12, barrier.getTimestamp());
				buf.putInt(20, checkpointType.ordinal());
			} else if (checkpointType == CheckpointType.SAVEPOINT) {
				String targetLocation = checkpointOptions.getTargetLocation();
				assert (targetLocation != null);
				byte[] locationBytes = targetLocation.getBytes(STRING_CODING_CHARSET);

				buf = ByteBuffer.allocate(24 + 4 + locationBytes.length);
				buf.putInt(0, CHECKPOINT_BARRIER_EVENT);
				buf.putLong(4, barrier.getId());
				buf.putLong(12, barrier.getTimestamp());
				buf.putInt(20, checkpointType.ordinal());
				buf.putInt(24, locationBytes.length);
				for (int i = 0; i < locationBytes.length; i++) {
					buf.put(28 + i, locationBytes[i]);
				}
			} else {
				throw new IOException("Unknown checkpoint type: " + checkpointType);
			}

			return buf;
		} else if (eventClass == EndOfSuperstepEvent.class) {
			return ByteBuffer.wrap(new byte[]{0, 0, 0, END_OF_SUPERSTEP_EVENT});
		} else if (eventClass == StartMigrationMarker.class) {

			StartMigrationMarker marker = (StartMigrationMarker) event;

			Map<ExecutionAttemptID, Set<Integer>> spillingVertices = marker.getSpillingVertices();
			Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices = marker.getStoppingVertices();

			int bufferSize = 36;
			bufferSize += spillingVertices.keySet().size() * 16;
			for (Set<Integer> integers : spillingVertices.values()) {
				bufferSize += integers.size() * 4 + 4;
			}
			bufferSize += stoppingVertices.keySet().size() * 16;
			for (List<InputChannelDeploymentDescriptor> list : stoppingVertices.values()) {

				bufferSize += 4;

				for (InputChannelDeploymentDescriptor icdd : list) {

					bufferSize += 33;

					if (icdd.getConsumedPartitionLocation().isRemote()) {
						bufferSize += 12;

						bufferSize += icdd.getConsumedPartitionLocation().getConnectionId().getAddress()
							.getHostName().getBytes().length;
					}
				}
			}

			int spillingSize = spillingVertices.keySet().size(), stoppingSize = stoppingVertices.size();

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(MIGRATION_START_EVENT);
			buf.putLong(marker.getModificationID());
			buf.putLong(marker.getTimestamp());
			buf.putLong(marker.getCheckpointIDToModify());
			buf.putInt(spillingSize);

			for (Map.Entry<ExecutionAttemptID, Set<Integer>> entry : spillingVertices.entrySet()) {

				buf.putInt(entry.getValue().size());

				ExecutionAttemptID vertexID = entry.getKey();
				buf.putLong(vertexID.getLowerPart());
				buf.putLong(vertexID.getUpperPart());

				for (Integer integer : entry.getValue()) {
					buf.putInt(integer);
				}
			}

			buf.putInt(stoppingSize);

			for (Map.Entry<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> entry : stoppingVertices.entrySet()) {

				ExecutionAttemptID vertexID = entry.getKey();
				buf.putLong(vertexID.getLowerPart());
				buf.putLong(vertexID.getUpperPart());

				buf.putInt(entry.getValue().size());

				for (InputChannelDeploymentDescriptor icdd : entry.getValue()) {
					ResultPartitionID partitionId = icdd.getConsumedPartitionId();

					buf.putLong(partitionId.getPartitionId().getLowerPart());
					buf.putLong(partitionId.getPartitionId().getUpperPart());

					buf.putLong(partitionId.getProducerId().getLowerPart());
					buf.putLong(partitionId.getProducerId().getUpperPart());

					ResultPartitionLocation location = icdd.getConsumedPartitionLocation();
					buf.put((byte) (location.isLocal() ? 1 : 0));

					if (location.isRemote()) {
						InetSocketAddress inetSocketAddress = location.getConnectionId().getAddress();

						byte[] hostName = inetSocketAddress.getHostName().getBytes();
						int hostNameSize = hostName.length;

						buf.putInt(hostNameSize);
						for (byte addres : hostName) {
							buf.put(addres);
						}

						buf.putInt(inetSocketAddress.getPort());

						buf.putInt(location.getConnectionId().getConnectionIndex());
					}
				}
			}

			buf.position(0);

			return buf;
		} else if (eventClass == CancelCheckpointMarker.class) {
			CancelCheckpointMarker marker = (CancelCheckpointMarker) event;

			ByteBuffer buf = ByteBuffer.allocate(12);
			buf.putInt(0, CANCEL_CHECKPOINT_MARKER_EVENT);
			buf.putLong(4, marker.getCheckpointId());
			return buf;
		} else if (eventClass == StartModificationMarker.class) {
			StartModificationMarker marker = (StartModificationMarker) event;

			Set<ExecutionAttemptID> executionAttemptIDS = marker.getJobVertexIDs();
			Set<Integer> subTaskIndices = marker.getSubTasksToPause();

			// 28 for all base field and dynamic size for the list of objects
			int bufferSize = executionAttemptIDS.size() * 16 + subTaskIndices.size() * 4 + 32;

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(0, MODIFICATION_START_EVENT);
			buf.putLong(4, marker.getModificationID());
			buf.putLong(12, marker.getTimestamp());
			buf.putInt(20, marker.getModificationAction().ordinal());
			buf.putInt(24, executionAttemptIDS.size());
			buf.putInt(28, subTaskIndices.size());

			ExecutionAttemptID[] executionAttempts =
				executionAttemptIDS.toArray(new ExecutionAttemptID[executionAttemptIDS.size()]);

			for (int index = 32, i = 0; i < executionAttemptIDS.size(); i++, index += 16) {
				ExecutionAttemptID vertexID = executionAttempts[i];
				buf.putLong(index, vertexID.getLowerPart());
				buf.putLong(index + 8, vertexID.getUpperPart());
			}

			Integer[] subTaskIndicesArray = subTaskIndices.toArray(new Integer[subTaskIndices.size()]);

			for (int index = 32 + executionAttemptIDS.size() * 16, i = 0; i < subTaskIndices.size(); i++, index += 4) {
				Integer taskIndex = subTaskIndicesArray[i];
				buf.putInt(index, taskIndex);
			}

			return buf;

		} else if (eventClass == CancelModificationMarker.class) {
			CancelModificationMarker marker = (CancelModificationMarker) event;

			Set<ExecutionAttemptID> executionAttemptIDS = marker.getJobVertexIDs();

			// 24 for all base field and dynamic size for the list of objects
			int bufferSize = executionAttemptIDS.size() * 16 + 24;

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(0, MODIFICATION_CANCEL_EVENT);
			buf.putLong(4, marker.getModificationID());
			buf.putLong(12, marker.getTimestamp());
			buf.putInt(20, executionAttemptIDS.size());

			ExecutionAttemptID[] executionAttempts =
				executionAttemptIDS.toArray(new ExecutionAttemptID[executionAttemptIDS.size()]);

			for (int index = 24, i = 0; i < executionAttemptIDS.size(); i++, index += 16) {
				ExecutionAttemptID executionAttemptID = executionAttempts[i];
				buf.putLong(index, executionAttemptID.getLowerPart());
				buf.putLong(index + 8, executionAttemptID.getUpperPart());
			}

			return buf;
		} else {
			try {
				final DataOutputSerializer serializer = new DataOutputSerializer(128);
				serializer.writeInt(OTHER_EVENT);
				serializer.writeUTF(event.getClass().getName());
				event.write(serializer);
				return serializer.wrapAsByteBuffer();
			} catch (IOException e) {
				throw new IOException("Error while serializing event.", e);
			}
		}
	}

	/**
	 * Identifies whether the given buffer encodes the given event.
	 *
	 * <p><strong>Pre-condition</strong>: This buffer must encode some event!</p>
	 *
	 * @param buffer the buffer to peak into
	 * @param eventClass the expected class of the event type
	 * @param classLoader the class loader to use for custom event classes
	 * @return whether the event class of the <tt>buffer</tt> matches the given <tt>eventClass</tt>
	 * @throws IOException
	 */
	private static boolean isEvent(ByteBuffer buffer, Class<?> eventClass, ClassLoader classLoader) throws IOException {
		if (buffer.remaining() < 4) {
			throw new IOException("Incomplete event");
		}

		final int bufferPos = buffer.position();
		final ByteOrder bufferOrder = buffer.order();
		buffer.order(ByteOrder.BIG_ENDIAN);

		try {
			int type = buffer.getInt();

			switch (type) {
				case END_OF_PARTITION_EVENT:
					return eventClass.equals(EndOfPartitionEvent.class);
				case CHECKPOINT_BARRIER_EVENT:
					return eventClass.equals(CheckpointBarrier.class);
				case END_OF_SUPERSTEP_EVENT:
					return eventClass.equals(EndOfSuperstepEvent.class);
				case CANCEL_CHECKPOINT_MARKER_EVENT:
					return eventClass.equals(CancelCheckpointMarker.class);
				case MODIFICATION_START_EVENT:
					return eventClass.equals(StartModificationMarker.class);
				case MODIFICATION_CANCEL_EVENT:
					return eventClass.equals(CancelModificationMarker.class);
				case SPILL_TO_DISK_MARKER:
					return eventClass.equals(SpillToDiskMarker.class);
				case PAUSING_TASK_EVENT:
					return eventClass.equals(PausingTaskEvent.class);
				case PAUSED_OPERATOR_EVENT:
					return eventClass.equals(PausingOperatorMarker.class);
				case MIGRATION_START_EVENT:
					return eventClass.equals(StartMigrationMarker.class);
				case OTHER_EVENT:
					try {
						final DataInputDeserializer deserializer = new DataInputDeserializer(buffer);
						final String className = deserializer.readUTF();

						final Class<? extends AbstractEvent> clazz;
						try {
							clazz = classLoader.loadClass(className).asSubclass(AbstractEvent.class);
						}
						catch (ClassNotFoundException e) {
							throw new IOException("Could not load event class '" + className + "'.", e);
						}
						catch (ClassCastException e) {
							throw new IOException("The class '" + className + "' is not a valid subclass of '"
								+ AbstractEvent.class.getName() + "'.", e);
						}
						return eventClass.equals(clazz);
					}
					catch (Exception e) {
						throw new IOException("Error while deserializing or instantiating event.", e);
					}
				default:
					throw new IOException("Corrupt byte stream for event");
			}
		}
		finally {
			buffer.order(bufferOrder);
			// restore the original position in the buffer (recall: we only peak into it!)
			buffer.position(bufferPos);
		}
	}

	public static AbstractEvent fromSerializedEvent(ByteBuffer buffer, ClassLoader classLoader) throws IOException {
		if (buffer.remaining() < 4) {
			throw new IOException("Incomplete event");
		}

		final ByteOrder bufferOrder = buffer.order();
		buffer.order(ByteOrder.BIG_ENDIAN);

		try {
			int type = buffer.getInt();

			if (type == END_OF_PARTITION_EVENT) {
				return EndOfPartitionEvent.INSTANCE;
			} else if (type == SPILL_TO_DISK_MARKER) {

				ModificationCoordinator.ModificationAction action =
					ModificationCoordinator.ModificationAction.values()[buffer.getInt()];

				return new SpillToDiskMarker(action);
			} else if (type == PAUSED_OPERATOR_EVENT) {

				byte containsLocation = buffer.get();

				if (containsLocation == 1) {

					long lower = buffer.getLong();
					long upper = buffer.getLong();

					IntermediateResultPartitionID irpID = new IntermediateResultPartitionID(lower, upper);

					lower = buffer.getLong();
					upper = buffer.getLong();

					ExecutionAttemptID attemptID = new ExecutionAttemptID(lower, upper);

					ResultPartitionID id = new ResultPartitionID(irpID, attemptID);

					boolean local = buffer.get() == 1;

					ResultPartitionLocation location;

					if (local) {
						location = ResultPartitionLocation.createLocal();
					} else {

						int addressSize = buffer.getInt();
						byte[] address = new byte[addressSize];
						for (int k = 0; k < addressSize; k++) {
							address[k] = buffer.get();
						}

						int port = buffer.getInt();

						InetSocketAddress inetSocketAddress = new InetSocketAddress(new String(address), port);

						int connectionIndex = buffer.getInt();

						ConnectionID connectionID = new ConnectionID(inetSocketAddress, connectionIndex);

						location = ResultPartitionLocation.createRemote(connectionID);
					}

					return new PausingOperatorMarker(new InputChannelDeploymentDescriptor(id, location));
				}

				return new PausingOperatorMarker();

			} else if (type == PAUSING_TASK_EVENT) {
				int subTaskIndex = buffer.getInt();
				long upcomingModificationID = buffer.getLong();

				return new PausingTaskEvent(subTaskIndex, upcomingModificationID);
			} else if (type == CHECKPOINT_BARRIER_EVENT) {
				long id = buffer.getLong();
				long timestamp = buffer.getLong();

				CheckpointOptions checkpointOptions;

				int checkpointTypeOrdinal = buffer.getInt();
				Preconditions.checkElementIndex(type, CheckpointType.values().length, "Illegal CheckpointType ordinal");
				CheckpointType checkpointType = CheckpointType.values()[checkpointTypeOrdinal];

				if (checkpointType == CheckpointType.FULL_CHECKPOINT) {
					checkpointOptions = CheckpointOptions.forFullCheckpoint();
				} else if (checkpointType == CheckpointType.SAVEPOINT) {
					int len = buffer.getInt();
					byte[] bytes = new byte[len];
					buffer.get(bytes);
					String targetLocation = new String(bytes, STRING_CODING_CHARSET);

					checkpointOptions = CheckpointOptions.forSavepoint(targetLocation);
				} else {
					throw new IOException("Unknown checkpoint type: " + checkpointType);
				}

				return new CheckpointBarrier(id, timestamp, checkpointOptions);
			} else if (type == END_OF_SUPERSTEP_EVENT) {
				return EndOfSuperstepEvent.INSTANCE;
			} else if (type == CANCEL_CHECKPOINT_MARKER_EVENT) {
				long id = buffer.getLong();
				return new CancelCheckpointMarker(id);
			} else if (type == MODIFICATION_START_EVENT) {

				long modificationID = buffer.getLong();
				long timestamp = buffer.getLong();
				ModificationCoordinator.ModificationAction action =
					ModificationCoordinator.ModificationAction.values()[buffer.getInt()];
				int instancesToPauseSize = buffer.getInt();
				int subTaskIndicesSize = buffer.getInt();

				Set<ExecutionAttemptID> ids = new HashSet<>(instancesToPauseSize);

				for (int i = 0; i < instancesToPauseSize; i++) {
					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ids.add(new ExecutionAttemptID(lower, upper));
				}

				Set<Integer> subTasksToPause = new HashSet<>(subTaskIndicesSize);

				for (int i = 0; i < subTaskIndicesSize; i++) {
					int index = buffer.getInt();
					subTasksToPause.add(index);
				}

				return new StartModificationMarker(modificationID, timestamp, ids, subTasksToPause, action);

			} else if (type == MODIFICATION_CANCEL_EVENT) {

				long modificationID = buffer.getLong();
				long timestamp = buffer.getLong();
				int size = buffer.getInt();

				Set<ExecutionAttemptID> ids = new HashSet<>(size);

				for (int i = 0; i < size; i++) {
					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ids.add(new ExecutionAttemptID(lower, upper));
				}

				return new CancelModificationMarker(modificationID, timestamp, ids);

			} else if (type == MIGRATION_START_EVENT) {

				Map<ExecutionAttemptID, Set<Integer>> spillingVertices = new HashMap<>();
				Map<ExecutionAttemptID, List<InputChannelDeploymentDescriptor>> stoppingVertices = new HashMap<>();

				long modificationID = buffer.getLong();
				long timestamp = buffer.getLong();
				long checkpointToModify = buffer.getLong();
				int size = buffer.getInt();

				for (int i = 0; i < size; i++) {
					int setSize = buffer.getInt();

					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(lower, upper);

					Set<Integer> set = new HashSet<>();
					for (int j = 0; j < setSize; j++) {
						set.add(buffer.getInt());
					}

					spillingVertices.put(executionAttemptID, set);
				}

				size = buffer.getInt();

				for (int i = 0; i < size; i++) {

					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ExecutionAttemptID executionAttemptID = new ExecutionAttemptID(lower, upper);

					int icddSize = buffer.getInt();

					List<InputChannelDeploymentDescriptor> list = new ArrayList<>();

					for (int j = 0; j < icddSize; j++) {

						lower = buffer.getLong();
						upper = buffer.getLong();

						IntermediateResultPartitionID irpID = new IntermediateResultPartitionID(lower, upper);

						lower = buffer.getLong();
						upper = buffer.getLong();

						ExecutionAttemptID attemptID = new ExecutionAttemptID(lower, upper);

						ResultPartitionID id = new ResultPartitionID(irpID, attemptID);

						boolean local = buffer.get() == 1;

						ResultPartitionLocation location;

						if (local) {
							location = ResultPartitionLocation.createLocal();
						} else {

							int addressSize = buffer.getInt();
							byte[] address = new byte[addressSize];
							for (int k = 0; k < addressSize; k++) {
								address[k] = buffer.get();
							}

							int port = buffer.getInt();

							InetSocketAddress inetSocketAddress = new InetSocketAddress(new String(address), port);

							int connectionIndex = buffer.getInt();

							ConnectionID connectionID = new ConnectionID(inetSocketAddress, connectionIndex);

							location = ResultPartitionLocation.createRemote(connectionID);
						}

						InputChannelDeploymentDescriptor descriptor = new InputChannelDeploymentDescriptor(id, location);

						list.add(descriptor);
					}

					stoppingVertices.put(executionAttemptID, list);
				}

				return new StartMigrationMarker(modificationID, timestamp, spillingVertices, stoppingVertices, checkpointToModify);
			} else if (type == OTHER_EVENT) {
				try {
					final DataInputDeserializer deserializer = new DataInputDeserializer(buffer);
					final String className = deserializer.readUTF();

					final Class<? extends AbstractEvent> clazz;
					try {
						clazz = classLoader.loadClass(className).asSubclass(AbstractEvent.class);
					} catch (ClassNotFoundException e) {
						throw new IOException("Could not load event class '" + className + "'.", e);
					} catch (ClassCastException e) {
						throw new IOException("The class '" + className + "' is not a valid subclass of '"
							+ AbstractEvent.class.getName() + "'.", e);
					}

					final AbstractEvent event = InstantiationUtil.instantiate(clazz, AbstractEvent.class);
					event.read(deserializer);

					return event;
				} catch (Exception e) {
					throw new IOException("Error while deserializing or instantiating event.", e);
				}
			} else {
				throw new IOException("Corrupt byte stream for event");
			}
		}
		finally {
			buffer.order(bufferOrder);
		}
	}

	// ------------------------------------------------------------------------
	// Buffer helpers
	// ------------------------------------------------------------------------

	public static Buffer toBuffer(AbstractEvent event) throws IOException {
		final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

		MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

		final Buffer buffer = new Buffer(data, FreeingBufferRecycler.INSTANCE, false);
		buffer.setSize(serializedEvent.remaining());

		return buffer;
	}

	public static AbstractEvent fromBuffer(Buffer buffer, ClassLoader classLoader) throws IOException {
		return fromSerializedEvent(buffer.getNioBuffer(), classLoader);
	}

	/**
	 * Identifies whether the given buffer encodes the given event.
	 *
	 * @param buffer the buffer to peak into
	 * @param eventClass the expected class of the event type
	 * @param classLoader the class loader to use for custom event classes
	 * @return whether the event class of the <tt>buffer</tt> matches the given <tt>eventClass</tt>
	 * @throws IOException
	 */
	public static boolean isEvent(final Buffer buffer,
								  final Class<?> eventClass,
								  final ClassLoader classLoader) throws IOException {
		return !buffer.isBuffer() &&
			isEvent(buffer.getNioBuffer(), eventClass, classLoader);
	}
}
