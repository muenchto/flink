package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by tobiasmuench on 11.12.18.
 */
public class SpillingAdaptSpanRecDeserializerAndDecompressor<T extends DeserializationDelegate<StreamElement>, IN> extends SpillingAdaptiveSpanningRecordDeserializer<T> {

    private HashMap<Integer, IN> dictionary;

    public SpillingAdaptSpanRecDeserializerAndDecompressor(String[] tmpDirectories) {
        super(tmpDirectories);
        this.dictionary = new HashMap<>();
    }

    @Override
    public DeserializationResult getNextRecord(T target) throws IOException {
        // always check the non-spanning wrapper first.
        // this should be the majority of the cases for small records
        // for large records, this portion of the work is very small in comparison anyways

        int nonSpanningRemaining = this.nonSpanningWrapper.remaining();

        // check if we can get a full length;
        if (nonSpanningRemaining >= 4) {
            int len = this.nonSpanningWrapper.readInt();

            if (len <= nonSpanningRemaining - 4) {
                // we can get a full record from here
                try {
                    target.read(this.nonSpanningWrapper);

                    decompress(target);

                    int remaining = this.nonSpanningWrapper.remaining();
                    if (remaining > 0) {
                        return DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
                    }
                    else if (remaining == 0) {
                        return DeserializationResult.LAST_RECORD_FROM_BUFFER;
                    }
                    else {
                        throw new IndexOutOfBoundsException("Remaining = " + remaining);
                    }
                }
                catch (IndexOutOfBoundsException e) {
                    throw new IOException(BROKEN_SERIALIZATION_ERROR_MESSAGE, e);
                }
            }
            else {
                // we got the length, but we need the rest from the spanning deserializer
                // and need to wait for more buffers
                this.spanningWrapper.initializeWithPartialRecord(this.nonSpanningWrapper, len);
                this.nonSpanningWrapper.clear();
                return DeserializationResult.PARTIAL_RECORD;
            }
        } else if (nonSpanningRemaining > 0) {
            // we have an incomplete length
            // add our part of the length to the length buffer
            this.spanningWrapper.initializeWithPartialLength(this.nonSpanningWrapper);
            this.nonSpanningWrapper.clear();
            return DeserializationResult.PARTIAL_RECORD;
        }

        // spanning record case
        if (this.spanningWrapper.hasFullRecord()) {
            // get the full record
            target.read(this.spanningWrapper.getInputView());

            decompress(target);

            // move the remainder to the non-spanning wrapper
            // this does not copy it, only sets the memory segment
            this.spanningWrapper.moveRemainderToNonSpanningDeserializer(this.nonSpanningWrapper);
            this.spanningWrapper.clear();

            return (this.nonSpanningWrapper.remaining() == 0) ?
                    DeserializationResult.LAST_RECORD_FROM_BUFFER :
                    DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
        } else {
            return DeserializationResult.PARTIAL_RECORD;
        }
    }

    private void decompress(T target) {
        StreamElement record = target.getInstance();
        if (record.isCompressedStreamRecord()) {
            CompressedStreamRecord compressedRecord = record.asCompressedStreamRecord();
            Integer comprKey = compressedRecord.compressedValue;

            IN uncompressedValue = dictionary.get(comprKey);
            if (compressedRecord.hasTimestamp) {
                target.setInstance(new StreamRecord<IN>(uncompressedValue, compressedRecord.timestamp));
            }
            else {
                target.setInstance(new StreamRecord<IN>(uncompressedValue));
            }
        }
        else if (record.isDictCompressionEntry()) {
            DictCompressionEntry<IN> newDictEntry = record.asDictCompressionEntry();


            if (newDictEntry.hasTimestamp) {
                target.setInstance(new StreamRecord<IN>(newDictEntry.value, newDictEntry.timestamp));
            }
            else {
                target.setInstance(new StreamRecord<IN>(newDictEntry.value));
            }

            dictionary.put(newDictEntry.key, newDictEntry.value);
        }
    }

}
