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
    public final int key;
    public final T value;

    public DictCompressionEntry(long timestamp, int key, T value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.hasTimestamp = true;
    }

    public DictCompressionEntry(int key, T value) {
        this.key = key;
        this.value = value;
        this.hasTimestamp = false;
    }

    //private Tuple2<Integer, T> entry;
//
    //public DictCompressionEntry(Integer key, T value) {
    //    this.entry = new Tuple2<>(key, value);
    //}
    //public DictCompressionEntry(Tuple2<Integer, T> tuple) {
    //    this.entry = tuple;
    //}
//
    //public Integer getKey() {
    //    return entry.f0;
    //}
//
    //public Tuple2<Integer, T> getEntry() {
    //    return entry;
    //}
//
    //public T getInternalValue() {return entry.f1;}


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
        return String.format("DictCompressionEntry with key: %i -> value: %s",
                key, value.toString());
    }
}
