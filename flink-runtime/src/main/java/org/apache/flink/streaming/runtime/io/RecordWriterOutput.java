/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.optimization.CompressionMarker;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link Output} that sends data using a {@link RecordWriter}.
 */
@Internal
public class RecordWriterOutput<OUT> implements Output<StreamRecord<OUT>> {

	protected static final Logger LOG = LoggerFactory.getLogger(RecordWriterOutput.class);

	private StreamRecordWriter<SerializationDelegate<StreamElement>, OUT> recordWriter;

	private SerializationDelegate<StreamElement> serializationDelegate;

	private final StreamStatusProvider streamStatusProvider;

	private final OutputTag outputTag;

	private final String name;

	@Override
	public String toString() {
		return getClass().getSimpleName() + " - " + name	;
//			+ " - StreamRecordWriter: " + recordWriter
//			+ " - SerializationDelegate: " + serializationDelegate
//			+ " - StreamStatusProvider:  " + streamStatusProvider
//			+ " - OutputTag: " + outputTag;
	}

	@SuppressWarnings("unchecked")
	public RecordWriterOutput(
		StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>, OUT> recordWriter,
		TypeSerializer<OUT> outSerializer,
		OutputTag outputTag,
		StreamStatusProvider streamStatusProvider, String name) {

		this.name = name;

		checkNotNull(recordWriter);
		this.outputTag = outputTag;
		// generic hack: cast the writer to generic Object type so we can use it
		// with multiplexed records and watermarks
		this.recordWriter = (StreamRecordWriter<SerializationDelegate<StreamElement>, OUT>)
				(StreamRecordWriter<?, OUT>) recordWriter;

		TypeSerializer<StreamElement> outRecordSerializer =
				new StreamElementSerializer<>(outSerializer);

		if (outSerializer != null) {
			serializationDelegate = new SerializationDelegate<StreamElement>(outRecordSerializer);
		}

		this.streamStatusProvider = checkNotNull(streamStatusProvider);

		LOG.info("Creating RecordWriterOutput for " + name);
	}

	public void enableCompressionMode() {
		broadcastCompressionMarker(new CompressionMarker().asEnabler());

		this.recordWriter.enableCompressionMode();

		LOG.debug("RecordWriterOutput of {} switched in Compression Mode", name);
	}

	public void disableCompressionMode() {
		broadcastCompressionMarker(new CompressionMarker().asDisabler());

		this.recordWriter.disableCompressionMode();

		LOG.debug("RecordWriterOutput of {} switched in NO-Compression Mode", name);
	}

	public StreamRecordWriter<SerializationDelegate<StreamElement>, OUT> getRecordWriter() {
		return recordWriter;
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		if (this.outputTag != null) {
			// we are only responsible for emitting to the main input
			return;
		}

		pushToRecordWriter(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
			// we are only responsible for emitting to the side-output specified by our
			// OutputTag.
			return;
		}

		pushToRecordWriter(record);
	}

	private <X> void pushToRecordWriter(StreamRecord<X> record) {
		serializationDelegate.setInstance(record);

		LOG.info("From {} sending record {} to {}.", name, record, recordWriter);

		try {
			recordWriter.emit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		serializationDelegate.setInstance(mark);

		if (streamStatusProvider.getStreamStatus().isActive()) {
			try {
				recordWriter.broadcastEmit(serializationDelegate);
			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

	public void emitStreamStatus(StreamStatus streamStatus) {
		serializationDelegate.setInstance(streamStatus);

		try {
			recordWriter.broadcastEmit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		serializationDelegate.setInstance(latencyMarker);

		try {
			recordWriter.randomEmit(serializationDelegate);
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private void broadcastCompressionMarker(CompressionMarker compressionMarker) {
		serializationDelegate.setInstance(compressionMarker);

		try {
			recordWriter.broadcastEmit(serializationDelegate);
			recordWriter.flush();
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		recordWriter.broadcastEvent(event);
	}

	public void sendToTarget(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		recordWriter.sendEventToTarget(event, targetChannel);
	}

	public void flush() throws IOException {
		recordWriter.flush();
	}

	@Override
	public void close() {
		recordWriter.close();
	}

	public void clearBuffers() {
		recordWriter.clearBuffers();
	}
}
