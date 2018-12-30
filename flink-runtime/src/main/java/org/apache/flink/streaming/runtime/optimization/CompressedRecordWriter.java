package org.apache.flink.streaming.runtime.optimization;

import com.google.common.primitives.Ints;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by tobiasmuench on 28.12.18.
 */
public class CompressedRecordWriter<T extends SerializationDelegate> extends StreamRecordWriter<T> {

    private HashMap<Integer, boolean[]> dictionary;


    public CompressedRecordWriter(StreamTask streamTask, ResultPartitionWriter writer, ChannelSelector channelSelector, long timeout, String name) {
        super(streamTask, writer, channelSelector, timeout, name);
        this.dictionary = new HashMap<>();
    }

    @Override
    public void emit(T record) throws IOException, InterruptedException {

        int[] selectChannels = super.getChannelSelector().selectChannels(record, super.getNumChannels());

        LOG.debug("Task {} writes buffer {} to channels {}.",
                targetPartition.getResultPartition().owningTaskName, record, Ints.join(",", selectChannels));

            for (int targetChannel : selectChannels) {
                if (record.getInstance() instanceof StreamRecord) {
                    this.compressRecord(record, targetChannel);
                }
                this.sendToTarget(record, targetChannel);
            }
    }

    private void compressRecord(SerializationDelegate<StreamElement> record, int targetChannel) throws IOException, InterruptedException {

        StreamRecord innerRecord = record.getInstance().asRecord();
        Integer hashComprRec = System.identityHashCode(innerRecord.getValue());

        if (!dictionary.containsKey(hashComprRec)) {
            boolean[] channelIndicator = new boolean[super.getNumChannels()];
            dictionary.put(hashComprRec, channelIndicator);
        }


        if (!dictionary.get(hashComprRec)[targetChannel]){

            DictCompressionEntry dictEntry;
            if (innerRecord.hasTimestamp()) {
                dictEntry = new DictCompressionEntry(innerRecord.getTimestamp(), hashComprRec, innerRecord.getValue());
            }
            else {
                dictEntry = new DictCompressionEntry(hashComprRec, innerRecord.getValue());
            }
            record.setInstance(dictEntry);
            dictionary.get(hashComprRec)[targetChannel] = true;
        }
        else {
            CompressedStreamRecord comprRecord;
            if (innerRecord.hasTimestamp()) {
                comprRecord = new CompressedStreamRecord(innerRecord.getTimestamp(), hashComprRec);
            }
            else {
                comprRecord = new CompressedStreamRecord(hashComprRec);
            }

            record.setInstance(comprRecord);
        }

    }
}
