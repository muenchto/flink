package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.streaming.runtime.optimization.util.LRUdictionary;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamInputDecompressor<IN> {

	private LRUdictionary<Long, IN> dictionary;

	protected static final Logger LOG = LoggerFactory.getLogger(StreamInputDecompressor.class);

	public StreamInputDecompressor() {
		this.dictionary = new LRUdictionary<>(1000);
	}

	public StreamRecord<IN> decompress(StreamElement record) {

		try {
			if (record.isCompressedStreamRecord()) {
				LOG.debug("SpillingAdaptSpanRecDeserializerAndDecompressor decompresses {}", record);

				CompressedStreamRecord compressedRecord = record.asCompressedStreamRecord();
				IN uncompressedValue = dictionary.get(compressedRecord.compressedValue);
				if (uncompressedValue == null) {
					throw new Exception("could not find key in dict of receiver ");
				}
				if (compressedRecord.hasTimestamp) {
					return new StreamRecord<IN>(uncompressedValue, compressedRecord.timestamp);
				}
				else {
					return new StreamRecord<IN>(uncompressedValue);
				}
			}
			else if (record.isDictCompressionEntry()) {
				LOG.debug("SpillingAdaptSpanRecDeserializerAndDecompressor decompresses {}", record);

				DictCompressionEntry<IN> newDictEntry = record.asDictCompressionEntry();

				dictionary.put(newDictEntry.key, newDictEntry.value);

				if (newDictEntry.hasTimestamp) {
					return new StreamRecord<IN>(newDictEntry.value, newDictEntry.timestamp);
				}
				else {
					return new StreamRecord<IN>(newDictEntry.value);
				}
			}
			else if (record.isRecord()){
				LOG.warn("Task in compression mode but received uncompressed stream element {}!", record);
				throw new Exception("Test Mode Exception: Task in compression mode but received uncompressed stream element " + record + "!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return record.asRecord();
	}
}
