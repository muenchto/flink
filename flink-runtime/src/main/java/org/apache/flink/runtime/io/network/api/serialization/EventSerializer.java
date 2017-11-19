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

import java.nio.charset.Charset;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointOptions.CheckpointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.*;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.DataInputDeserializer;
import org.apache.flink.runtime.util.DataOutputSerializer;
import org.apache.flink.streaming.runtime.modification.events.CancelModificationMarker;
import org.apache.flink.streaming.runtime.modification.events.StartModificationMarker;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

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

	// ------------------------------------------------------------------------

	public static ByteBuffer toSerializedEvent(AbstractEvent event) throws IOException {
		final Class<?> eventClass = event.getClass();
		if (eventClass == EndOfPartitionEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_PARTITION_EVENT });
		}
		else if (eventClass == SpillToDiskMarker.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, SPILL_TO_DISK_MARKER });
		}
		else if (eventClass == CheckpointBarrier.class) {
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
				assert(targetLocation != null);
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
		}
		else if (eventClass == EndOfSuperstepEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_SUPERSTEP_EVENT });
		}
		else if (eventClass == CancelCheckpointMarker.class) {
			CancelCheckpointMarker marker = (CancelCheckpointMarker) event;

			ByteBuffer buf = ByteBuffer.allocate(12);
			buf.putInt(0, CANCEL_CHECKPOINT_MARKER_EVENT);
			buf.putLong(4, marker.getCheckpointId());
			return buf;
		} else if (eventClass == StartModificationMarker.class) {
			StartModificationMarker marker = (StartModificationMarker) event;

			List<JobVertexID> jobVertexIDs = marker.getJobVertexIDs();

			// 24 for all base field and dynamic size for the list of objects
			int bufferSize = jobVertexIDs.size() * 16 + 24;

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(0, MODIFICATION_START_EVENT);
			buf.putLong(4, marker.getModificationID());
			buf.putLong(12, marker.getTimestamp());
			buf.putInt(20, jobVertexIDs.size());

			for (int index = 24, i = 0; i < jobVertexIDs.size(); i++, index += 16) {
				JobVertexID vertexID = jobVertexIDs.get(i);
				buf.putLong(index, vertexID.getLowerPart());
				buf.putLong(index + 8, vertexID.getUpperPart());
			}

			return buf;

		} else if (eventClass == CancelModificationMarker.class) {
			CancelModificationMarker marker = (CancelModificationMarker) event;

			List<JobVertexID> jobVertexIDs = marker.getJobVertexIDs();

			// 24 for all base field and dynamic size for the list of objects
			int bufferSize = jobVertexIDs.size() * 16 + 24;

			ByteBuffer buf = ByteBuffer.allocate(bufferSize);
			buf.putInt(0, MODIFICATION_CANCEL_EVENT);
			buf.putLong(4, marker.getModificationID());
			buf.putLong(12, marker.getTimestamp());
			buf.putInt(20, jobVertexIDs.size());

			for (int index = 24, i = 0; i < jobVertexIDs.size(); i++, index += 16) {
				JobVertexID vertexID = jobVertexIDs.get(i);
				buf.putLong(index, vertexID.getLowerPart());
				buf.putLong(index + 8, vertexID.getUpperPart());
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
				return SpillToDiskMarker.INSTANCE;
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
				int size = buffer.getInt();

				List<JobVertexID> ids = new ArrayList<>(size);

				for (int i = 0; i < size; i++) {
					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ids.add(new JobVertexID(lower, upper));
				}

				return new StartModificationMarker(modificationID, timestamp, ids);

			} else if (type == MODIFICATION_CANCEL_EVENT) {

				long modificationID = buffer.getLong();
				long timestamp = buffer.getLong();
				int size = buffer.getInt();

				List<JobVertexID> ids = new ArrayList<>(size);

				for (int i = 0; i < size; i++) {
					long lower = buffer.getLong();
					long upper = buffer.getLong();
					ids.add(new JobVertexID(lower, upper));
				}

				return new CancelModificationMarker(modificationID, timestamp, ids);

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
