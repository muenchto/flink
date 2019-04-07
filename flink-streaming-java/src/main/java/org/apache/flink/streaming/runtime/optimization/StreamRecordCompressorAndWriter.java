package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.optimization.util.LRUdictionary;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by tobiasmuench on 15.03.19.
 */
public class StreamRecordCompressorAndWriter<T extends IOReadableWritable, OUT> extends StreamRecordWriter<T, OUT>{

	protected static final Logger LOG = LoggerFactory.getLogger(StreamRecordCompressorAndWriter.class);

	private boolean compressionEnabled;
	private LRUdictionary<OUT, Tuple2<Long, boolean[]>> dictionary;
	private long dictKey;

	private final long originalTimeout;

	private final OptimizationConfig optiConfig;
	private float recordCntForAnalyzer;
	private int recordsUntilComprAnalysis;


	private int repetitionCnt;

	public StreamRecordCompressorAndWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, long timeout, String taskName, OptimizationConfig optimizationConfig) {
		super(writer, channelSelector, timeout, taskName);

		this.optiConfig = optimizationConfig;
		this.originalTimeout = timeout;

		this.dictionary = new LRUdictionary<>(optiConfig.getDictionarySize());

		this.recordsUntilComprAnalysis = optiConfig.getAnalyzeEveryNRecords();

		repetitionCnt = 0;
		recordCntForAnalyzer = 0;
		this.dictKey = 0;
		this.compressionEnabled = false;

	}

	@Override
	public void emit(T record) throws IOException, InterruptedException {

		// the generics do not specify if this is a StreamRecord. If not, we dont want to use it for the logic here
		if (record instanceof SerializationDelegate
				&& ((SerializationDelegate) record).getInstance() instanceof StreamRecord) {

			// we optimistically declare an ComprEntry here and do the casting
			// the logic ensures that the ComprEntry will be properly assigned if we use it later
			Tuple2<Long, boolean[]> possibleComprEntry = null;
			SerializationDelegate delegate = (SerializationDelegate) record;
			StreamRecord<OUT> innerRecord = ((StreamElement) delegate.getInstance()).asRecord();

			// this is expensive, so when compression is not enabled yet, only do it at a difined persentage of records
			recordsUntilComprAnalysis--;
			if (!compressionEnabled && recordsUntilComprAnalysis == 0) {
				possibleComprEntry = analyzeForCompression(innerRecord);
				recordsUntilComprAnalysis = optiConfig.getAnalyzeEveryNRecords();
			}
			else if (compressionEnabled){
				possibleComprEntry = analyzeForCompression(innerRecord);
			}

			// check again separately because compression might be enabled through analyzer in if-case above
			if (compressionEnabled){
				// compression happens per channel, therefore we need to call the ChannelSelector here and bypass the emit()
				// of both parent and grandparent

				//mirror the behaviour of super class, but add compression
				checkErroneous();
				int[] selectedChannels = super.channelSelector.selectChannels(record, super.numChannels);

				LOG.debug("Compressor writes {} to channels [{}].",
						delegate.getInstance(), Arrays.toString(selectedChannels));

				for (int targetChannel : selectedChannels) {
					compressRecord(delegate, possibleComprEntry, targetChannel);
					LOG.debug("Compressor compressed record to {}", delegate.getInstance());
					sendToTarget((T) delegate, targetChannel);
				}
			}
			else {
				super.emit(record);
			}
		}
		else {
			super.emit(record);
		}
	}

	private void enableCompressionMode() {

		super.outputFlusher.setTimeout(optiConfig.getTimeoutDuringCompression());
		broadcastCompressionMarker(new CompressionMarker().asEnabler());
		this.compressionEnabled = true;
	}

	private void disableCompressionMode() {

		super.outputFlusher.setTimeout(originalTimeout);
		broadcastCompressionMarker(new CompressionMarker().asDisabler());
		this.compressionEnabled = false;
	}

	private void broadcastCompressionMarker(CompressionMarker compressionMarker) {
		//dummy serializer, every serializer can serialize the marker
		TypeSerializer<StreamElement> dummySerializer =
				new StreamElementSerializer<>(new IntSerializer());
		SerializationDelegate<StreamElement> markerDelegate = new SerializationDelegate<StreamElement>(dummySerializer);
		markerDelegate.setInstance(compressionMarker);

		try {
			broadcastEmit((T) markerDelegate);
			flushAll();
		}
		catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	private Tuple2<Long, boolean[]> analyzeForCompression(StreamRecord<OUT> record) {

		// when compression is enabled, the current dictionary entry should be provided
		Tuple2<Long, boolean[]> returnEntry;

		// optimistically create an entry, but add only if this record was not seen before (the last dictionary.size() records)
		boolean[] channelIndicator = new boolean[super.numChannels];
		Tuple2<Long, boolean[]> optimisticNewEntry = new Tuple2<>(dictKey, channelIndicator);
		// this simulates a dictionary.containsKey()
		Tuple2<Long, boolean[]> optimisticExistingEntry = dictionary.putIfAbsent(record.getValue(), optimisticNewEntry);
		if (optimisticExistingEntry != null) {
			// record was not added, its a repetition!
			repetitionCnt++;
			returnEntry = optimisticExistingEntry;
		}
		else {
			// added new entry in the dictionary, update key
			dictKey++;
			returnEntry = optimisticNewEntry;
		}

		recordCntForAnalyzer++;
		if (recordCntForAnalyzer == optiConfig.getRepetitionWindow()) {
			// window completed
			LOG.debug("CompressionAnalyzer: {} repeated records in window with size {}",
					repetitionCnt, optiConfig.getRepetitionWindow());

			if (repetitionCnt / recordCntForAnalyzer > optiConfig.getCompressionThresholdPercentage()) {
				//e.g. 70% of the last 10 records have been repetitions
				if (!compressionEnabled) {
					LOG.info("CompressionAnalyzer turned on compression!");
					enableCompressionMode();
				}
			}
			else {
				if (compressionEnabled) {
					LOG.info("CompressionAnalyzer disabled compression!");
					disableCompressionMode();
				}
			}
			recordCntForAnalyzer = 0;
			repetitionCnt = 0;
		}

		if (compressionEnabled) {
			return returnEntry;
		}
		else {
			return null;
		}
	}

	private void compressRecord(SerializationDelegate<StreamElement> record, Tuple2<Long, boolean[]> compressionEntry, int targetChannel) throws IOException, InterruptedException {

		StreamRecord<OUT> innerRecord = record.getInstance().asRecord();
		OUT recordData = innerRecord.getValue();

		// if have optimized this lookup away
		//Tuple2<Long, boolean[]> entry = dictionary.get(recordData);

		if (compressionEntry.f1[targetChannel]){
			// the corresponding dictionary on the reciever side should know this dictKey - send compressed
			CompressedStreamRecord comprRecord;
			if (innerRecord.hasTimestamp()) {
				comprRecord = new CompressedStreamRecord(innerRecord.getTimestamp(), compressionEntry.f0);
			}
			else {
				comprRecord = new CompressedStreamRecord(compressionEntry.f0);
			}

			record.setInstance(comprRecord);
		}
		else {
			// this is the first time this record was send down this channel - send a new DictEntry
			DictCompressionEntry dictEntry;
			if (innerRecord.hasTimestamp()) {
				dictEntry = new DictCompressionEntry<OUT>(innerRecord.getTimestamp(), compressionEntry.f0, recordData);
			}
			else {
				dictEntry = new DictCompressionEntry<OUT>(compressionEntry.f0, recordData);
			}
			record.setInstance(dictEntry);
			compressionEntry.f1[targetChannel] = true;

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

	public static class LoggingSimpleCounter implements Counter {
		private Counter innerCounter;

		LoggingSimpleCounter(Counter innerCnt) {
			this.innerCounter = innerCnt;
		}

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
