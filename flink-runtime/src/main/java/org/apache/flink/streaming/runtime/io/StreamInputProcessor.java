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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.PausingOperatorMarker;
import org.apache.flink.runtime.io.network.api.SpillToDiskMarker;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.modification.events.CancelModificationMarker;
import org.apache.flink.streaming.runtime.modification.events.StartMigrationMarker;
import org.apache.flink.streaming.runtime.modification.events.StartModificationMarker;
import org.apache.flink.streaming.runtime.optimization.SpillingAdaptSpanRecDeserializerAndDecompressor;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link OneInputStreamOperator} concurrently with the timer callback or other things.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public class StreamInputProcessor<IN> {

	static final Logger LOG = LoggerFactory.getLogger(StreamInputProcessor.class);

	private RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final CheckpointBarrierHandler barrierHandler;

	private final Object lock;

	// ---------------- Status and Watermark Valve ------------------

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private StatusWatermarkValve statusWatermarkValve;

	/** Number of input channels the valve needs to handle. */
	private final int numInputChannels;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to channel indexes of the valve.
	 */
	private int currentChannel = -1;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final OneInputStreamOperator<IN, ?> streamOperator;

	// ---------------- Metrics ------------------

	private long lastEmittedWatermark;
	private Counter numRecordsIn;

	private boolean isFinished;

	private final StreamTask task;

	// ---------------- memorize for possible compression mode ------------------

	private final String[] spillingDirectoriesPaths;
	private RecordDeserializer<DeserializationDelegate<StreamElement>>[] savedSerializers;
	private boolean inCompressionMode;

	@SuppressWarnings("unchecked")
	public StreamInputProcessor(
			InputGate[] inputGates,
			TypeSerializer<IN> inputSerializer,
			StreamTask checkpointedTask,
			CheckpointingMode checkpointMode,
			Object lock,
			IOManager ioManager,
			Configuration taskManagerConfig,
			StreamStatusMaintainer streamStatusMaintainer,
			OneInputStreamOperator<IN, ?> streamOperator) throws IOException {

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		LOG.info("StreamInputProcessor: InputGate: " + inputGate);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
						TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
						+ " must be positive or -1 (infinite)");
			}
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager, maxAlign);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate, checkpointedTask.getName());
			LOG.info("StreamInputProcessor: AtLeastOnce");
		}
		else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}

		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}
		task = checkpointedTask;

		this.lock = checkNotNull(lock);

		StreamElementSerializer<IN> ser = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
					ioManager.getSpillingDirectoriesPaths());
		}

		this.spillingDirectoriesPaths = ioManager.getSpillingDirectoriesPaths();
		this.numInputChannels = inputGate.getNumberOfInputChannels();

		this.lastEmittedWatermark = Long.MIN_VALUE;

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.streamOperator = checkNotNull(streamOperator);

		this.statusWatermarkValve = new StatusWatermarkValve(
				numInputChannels,
				new ForwardingValveOutputHandler(streamOperator, lock));

		inCompressionMode = false;

		LOG.info("StreamInputProcessor: " + streamOperator.getClass().getSimpleName() + " with gate " + inputGate);
	}

	public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						// handle watermark
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
						continue;
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else if (recordOrMark.isCompressionMarker()) {
						synchronized (lock) {
							LOG.debug("{} received {} from channel {}",
									task.getName(), recordOrMark.asCompressionMarker(), currentChannel);
							if (recordOrMark.asCompressionMarker().isEnabler()) enableCompressionMode();
							else disableCompressionMode();
						}
						continue;
					}
					else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							LOG.debug("Task {} received {} from channel {}",
								task.getName(), record.getValue(), currentChannel);
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();

			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();

					if (event.getClass() == StartModificationMarker.class) {
						// Do nothing here, as modification was triggered in BarrierTracker
						// Return true, so that we can restart this method, but check if we are still running

						return true;
					} else if (event.getClass() == CancelModificationMarker.class) {
						// Do nothing here, as modification was triggered in BarrierTracker
						// Return true, so that we can restart this method, but check if we are still running
						return true;

					} else if (event.getClass() == SpillToDiskMarker.class) {
						// Do nothing here, as modification was triggered in BarrierTracker
						// Return true, so that we can restart this method, but check if we are still running

						return true;
					} else if (event.getClass() == StartMigrationMarker.class) {
						// Do nothing here, as modification was triggered in BarrierTracker
						// Return true, so that we can restart this method, but check if we are still running

						return true;
					} else if (event.getClass() == PausingOperatorMarker.class) {
						// Do nothing here, as modification was triggered in BarrierTracker
						// Return true, so that we can restart this method, but check if we are still running

						return true;
					} else if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}

	private void enableCompressionMode() {

		//make method idempotent because the InputProcessor can get several Enable Marker
		if (this.inCompressionMode) {
			return;
		}
		// Initialize for compression awareness
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDecompressors =
				new SpillingAdaptSpanRecDeserializerAndDecompressor[numInputChannels];

		for (int i = 0; i < recordDecompressors.length; i++) {
			recordDecompressors[i] =
					new SpillingAdaptSpanRecDeserializerAndDecompressor<DeserializationDelegate<StreamElement>, IN>(
							spillingDirectoriesPaths);
		}

		this.savedSerializers = recordDeserializers;
		this.recordDeserializers = recordDecompressors;


		//this will forward the compression mode activation to the RecordWriter who fill send markers down to
		// following tasks
		task.enableCompressionForTask();

		this.inCompressionMode = true;
	}

	private void disableCompressionMode() {

		//make method idempotent because the InputProcessor can get several Disable Marker
		if (!this.inCompressionMode) {
			return;
		}
		this.recordDeserializers = savedSerializers;
		this.inCompressionMode = false;
	}

	/**
	 * Sets the metric group for this StreamInputProcessor.
	 *
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return lastEmittedWatermark;
			}
		});

		metrics.gauge("checkpointAlignmentTime", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return barrierHandler.getAlignmentDurationNanos();
			}
		});
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private final OneInputStreamOperator<IN, ?> operator;
		private final Object lock;

		private ForwardingValveOutputHandler(final OneInputStreamOperator<IN, ?> operator, final Object lock) {
			this.operator = checkNotNull(operator);
			this.lock = checkNotNull(lock);
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark = watermark.getTimestamp();
					operator.processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					streamStatusMaintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}

	@Override
	public String toString() {
		return getClass().getName() + statusWatermarkValve + " - numInputChannels: " + numInputChannels
			+ " - currentChannel: " + currentChannel + " - OneInputStreamOperator: " + streamOperator;
	}
}
