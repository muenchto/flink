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

import com.google.common.primitives.Ints;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.optimization.CompressedStreamRecord;
import org.apache.flink.streaming.runtime.optimization.DictCompressionEntry;
import org.apache.flink.streaming.runtime.optimization.util.LRUdictionary;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This record writer keeps data in buffers at most for a certain timeout. It spawns a separate thread
 * that flushes the outputs in a defined interval, to make sure data does not linger in the buffers for too long.
 *
 * @param <T> The type of elements written.
 */
@Internal
public class StreamRecordWriter<T extends IOReadableWritable, OUT> extends RecordWriter<T> {

	/** Default name for teh output flush thread, if no name with a task reference is given. */
	private static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

	protected static final Logger LOG = LoggerFactory.getLogger(StreamRecordWriter.class);
	protected static final Logger BENCH = LoggerFactory.getLogger("numBytesOutLog");

	/** The thread that periodically flushes the output, to give an upper latency bound. */
	private final OutputFlusher outputFlusher;

	/** Flag indicating whether the output should be flushed after every element. */
	private final boolean flushAlways;
	public final String name;
	public final StreamTask streamTask;

	public final long timeout;

	/** The exception encountered in the flushing thread. */
	private Throwable flusherException;


	private boolean compressionEnabled;
	private LRUdictionary<OUT, Tuple2<Long, boolean[]>> dictionary;
	long key;


	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + name;
	}

	public StreamRecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, long timeout) {
		this(null, writer, channelSelector, timeout, null);
	}

	public StreamRecordWriter(StreamTask streamTask, ResultPartitionWriter writer, ChannelSelector<T> channelSelector,
							  long timeout, String taskName) {

		super(writer, channelSelector);

		this.streamTask = streamTask;

		LOG.info("{} Creating StreamRecordWriter for {} and {}", taskName, writer, channelSelector);

		this.name = taskName;

		checkArgument(timeout >= -1);

		if (timeout == -1) {
			flushAlways = false;
			outputFlusher = null;
		}
		else if (timeout == 0) {
			flushAlways = true;
			outputFlusher = null;
		}
		else {
			flushAlways = false;
			String threadName = taskName == null ?
								DEFAULT_OUTPUT_FLUSH_THREAD_NAME : "Output Timeout Flusher - " + taskName;

			outputFlusher = new OutputFlusher(threadName, timeout);
			outputFlusher.start();
		}
		this.timeout = timeout;

		this.compressionEnabled = false;
		this.dictionary = new LRUdictionary<>(1000);
		this.key = -1; //start negative so that we have 0 for first record
	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {
		checkErroneous();

		if (isStreamTaskPausing()) {
			return;
		}


		if (compressionEnabled){

			SerializationDelegate delegate = (SerializationDelegate) record;
			int[] selectChannels = super.getChannelSelector().selectChannels(record, super.getNumChannels());

			LOG.debug("Task {} writes {} to channels [{}].",
					name, delegate.getInstance(), Ints.join(",", selectChannels));

			for (int targetChannel : selectChannels) {
				if (delegate.getInstance() instanceof StreamRecord) {
					compressRecord(delegate, targetChannel);
					LOG.debug("Task {} compressed record to {}", name, delegate.getInstance());
				}
				sendToTarget((T) delegate, targetChannel);
			}
		}
		else {
			//LOG.info(name + " emits: " + record);
			super.emit(record);
		}

		if (flushAlways) {
			flush();
		}
	}

	public void sendEventToTarget(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		checkErroneous();

		if (isStreamTaskPausing()) {
			return;
		}

		LOG.info("{} sends {} to {}", name, event, targetChannel);

		super.sendEventToTarget(event, targetChannel);
		if (flushAlways) {
			flush();
		}
	}

	@Override
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();

		if (isStreamTaskPausing()) {
			return;
		}

		LOG.info(name + " broadcast emmits: " + record);

		super.broadcastEmit(record);
		if (flushAlways) {
			flush();
		}
	}

	@Override
	public void randomEmit(T record) throws IOException, InterruptedException {
		checkErroneous();

		if (isStreamTaskPausing()) {
			return;
		}

		LOG.info(name + " random emmits: " + record);

		super.randomEmit(record);
		if (flushAlways) {
			flush();
		}
	}

	@Override
	public void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		checkErroneous();

		if (isStreamTaskPausing()) {
			return;
		}

		//LOG.info(name + " send to target emmits: " + record + " - " + targetChannel);

		super.sendToTarget(record, targetChannel);
		if (flushAlways) {
			flush();
		}
	}

	/**
	 * Closes the writer. This stops the flushing thread (if there is one).
	 */
	public void close() {
		// make sure we terminate the thread in any case
		if (outputFlusher != null) {
			outputFlusher.terminate();
			try {
				outputFlusher.join();
			}
			catch (InterruptedException e) {
				// ignore on close
			}
		}
	}

	/**
	 * Notifies the writer that the output flusher thread encountered an exception.
	 *
	 * @param t The exception to report.
	 */
	private void notifyFlusherException(Throwable t) {
		if (this.flusherException == null) {
			this.flusherException = t;
		}
	}

	private void checkErroneous() throws IOException {
		if (flusherException != null) {
			throw new IOException("An exception happened while flushing the outputs", flusherException);
		}
	}

	private boolean isStreamTaskPausing() {
		if (streamTask.getPausedForModification()) {
			LOG.info("{} is pausing, therefore not sending more records or events.", name);
			return true;
		} else {
			return false;
		}
	}

	public void enableCompressionMode() {

		this.compressionEnabled = true;
	}
	public void disableCompressionMode() {
		this.compressionEnabled = false;
	}

	private void compressRecord(SerializationDelegate<StreamElement> record, int targetChannel) throws IOException, InterruptedException {

		StreamRecord<OUT> innerRecord = record.getInstance().asRecord();
		OUT recordData = innerRecord.getValue();

		if (!dictionary.containsKey(recordData)) {
			// this is the first time ever that we see this record, therefore create an entry in the HashMap first!
			boolean[] channelIndicator = new boolean[super.getNumChannels()];
			key++;
			dictionary.put(recordData, new Tuple2<>(key, channelIndicator));
		}

		Tuple2<Long, boolean[]> entry = dictionary.get(recordData);

		if (entry.f1[targetChannel]){
			// the corresponding dictionary on the reciever side should know this key - send compressed
			CompressedStreamRecord comprRecord;
			if (innerRecord.hasTimestamp()) {
				comprRecord = new CompressedStreamRecord(innerRecord.getTimestamp(), entry.f0);
			}
			else {
				comprRecord = new CompressedStreamRecord(entry.f0);
			}

			record.setInstance(comprRecord);
		}
		else {
			// this is the first time this record was send down this channel - send a new DictEntry
			DictCompressionEntry dictEntry;
			if (innerRecord.hasTimestamp()) {
				dictEntry = new DictCompressionEntry(innerRecord.getTimestamp(), entry.f0, recordData);
			}
			else {
				dictEntry = new DictCompressionEntry(entry.f0, recordData);
			}
			record.setInstance(dictEntry);
			entry.f1[targetChannel] = true;

		}

	}

	/**
	 * Sets the metric group for this StreamRecordWriter.
	 * Overrides the method from RecordWriter and wraps the Counter in order to enable special logging
	 * @param metrics
	 */
	@Override
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		super.numBytesOut = new LoggingSimpleCounter(metrics.getNumBytesOutCounter());
	}



	// ------------------------------------------------------------------------

	/**
	 * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
	 *
	 * <p>The thread is daemonic, because it is only a utility thread.
	 */
	private class OutputFlusher extends Thread {

		private final long timeout;

		private volatile boolean running = true;


		OutputFlusher(String name, long timeout) {
			super(name);
			setDaemon(true);
			this.timeout = timeout;
		}

		public void terminate() {
			running = false;
			interrupt();
		}

		@Override
		public void run() {
			try {
				while (running) {
					try {
						Thread.sleep(timeout);
					}
					catch (InterruptedException e) {
						// propagate this if we are still running, because it should not happen
						// in that case
						if (running) {
							throw new Exception(e);
						}
					}

					// any errors here should let the thread come to a halt and be
					// recognized by the writer
					flush();
				}
			}
			catch (Throwable t) {
				notifyFlusherException(t);
			}
		}
	}

	public static class LoggingSimpleCounter implements Counter {
		private Counter innerCounter;

		LoggingSimpleCounter(Counter innerCnt) {
			this.innerCounter = innerCnt;
		}

		/** the current count */
		private long count;

		/**
		 * Increment the current count by 1.
		 */
		@Override
		public void inc() {
			innerCounter.inc();
		}

		/**
		 * Increment the current count by the given value.
		 *
		 * @param n value to increment the current count by
		 */
		@Override
		public void inc(long n) {
			innerCounter.inc(n);
			BENCH.debug("{}; {}", System.currentTimeMillis(), innerCounter.getCount());


		}

		/**
		 * Decrement the current count by 1.
		 */
		@Override
		public void dec() {
			innerCounter.dec();
		}

		/**
		 * Decrement the current count by the given value.
		 *
		 * @param n value to decrement the current count by
		 */
		@Override
		public void dec(long n) {
			innerCounter.dec(n);
		}

		/**
		 * Returns the current count.
		 *
		 * @return current count
		 */
		@Override
		public long getCount() {
			return innerCounter.getCount();
		}
	}

}
