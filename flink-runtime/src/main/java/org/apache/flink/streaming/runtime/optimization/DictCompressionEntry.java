package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * Created by tobiasmuench on 12.12.18.
 */

/**
 * A wrapper to make the tuple, that holds the key and the StreamRecord of a DICT compression entry, a StreamElement
 * @param <T> the DataType of the StreamRecord<T> in the stream
 */
public class DictCompressionEntry<T> extends StreamElement {

    public final boolean hasTimestamp;
    public long timestamp;
    public final long key;
    public final T value;

    public DictCompressionEntry(long timestamp, long key, T value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.hasTimestamp = true;
    }

    public DictCompressionEntry(long key, T value) {
        this.key = key;
        this.value = value;
        this.hasTimestamp = false;
    }



    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    //
    //  These methods are inherited form the generic serialization of AbstractEvent
    //  but would require the CheckpointBarrier to be mutable. Since all serialization
    //  for events goes through the EventSerializer class, which has special serialization
    //  for the CheckpointBarrier, we don't need these methods
    //



    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "DictCompressionRecord@" + (hasTimestamp ? timestamp : "(noTS)") + " : " + key + " -> "
                + value.toString().substring(0, Math.min(value.toString().length(), 30));
    }
}
