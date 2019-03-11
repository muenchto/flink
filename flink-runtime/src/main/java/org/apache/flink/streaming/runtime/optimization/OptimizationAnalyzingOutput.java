package org.apache.flink.streaming.runtime.optimization;

import com.google.common.collect.EvictingQueue;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tobiasmuench on 15.01.19.
 */
public class OptimizationAnalyzingOutput<OUT> implements Output<StreamRecord<OUT>> {

    protected static final Logger LOG = LoggerFactory.getLogger(OptimizationAnalyzingOutput.class);


    private final StreamTask<?, ?> containingTask;
    private AbstractStreamOperator.CountingOutput countingOutput;
    private EvictingQueue<OUT> knownRecords;

    private boolean inCompressinMode;
    private float recordCnt;
    private final int WINDOW_SIZE = 10;
    private int repetitionCnt;

    public OptimizationAnalyzingOutput(AbstractStreamOperator.CountingOutput countingOutput, StreamTask<?, ?> containingTask) {
        this.countingOutput = countingOutput;
        this.containingTask = containingTask;

        knownRecords = EvictingQueue.create(1000);
        inCompressinMode = false;
        repetitionCnt = 0;
        recordCnt = 0;
    }

    @Override
    public void collect(StreamRecord<OUT> record) {
        analyzeForCompression(record);
        this.countingOutput.collect(record);
    }

    @Override
    public void close() {
        this.countingOutput.close();
    }

    @Override
    public void emitWatermark(Watermark mark) {
        this.countingOutput.emitWatermark(mark);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        countingOutput.collect(outputTag, record);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        this.countingOutput.emitLatencyMarker(latencyMarker);
    }

    private void analyzeForCompression(StreamRecord<OUT> record) {

        recordCnt++;

        if (knownRecords.contains(record.getValue())) {
            repetitionCnt++;
        }
        else {
            knownRecords.add(record.getValue());
        }

        if (recordCnt == WINDOW_SIZE) {
            // window completed
            LOG.debug("{} repeated records in window with size {}", repetitionCnt, WINDOW_SIZE);

            if (repetitionCnt/recordCnt > 0.7) {
                //i.e. 70% of the last 10 records have been repetitions
                if (!inCompressinMode) {
                    LOG.info("Compression analyzer turned on compression!");
                    containingTask.enableCompressionForTask();
                    inCompressinMode = true;
                }
            }
            else {
                if (inCompressinMode) {
                    LOG.info("Compression analyzer disabled compression!");
                    containingTask.disableCompressionMode();
                    inCompressinMode = false;
                }
            }
            recordCnt = 0;
            repetitionCnt = 0;
        }
    }
}
