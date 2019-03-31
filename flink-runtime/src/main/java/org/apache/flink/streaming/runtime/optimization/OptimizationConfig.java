package org.apache.flink.streaming.runtime.optimization;

/**
 * Created by tobiasmuench on 30.03.19.
 */
public class OptimizationConfig implements java.io.Serializable {
    private int timeoutDuringCompression;
    private int analyzeEveryNRecords;
    private int dictionarySize;
    private int repetitionWindow;
    private double compressionThresholdPercentage;

    private boolean isEnabled;

    public OptimizationConfig(int timeoutDuringCompression, int analyzeEveryNRecords, int dictionarySize, int repetitionWindow, int compressionThresholdPercentage) {
        this.timeoutDuringCompression = timeoutDuringCompression;
        this.analyzeEveryNRecords = analyzeEveryNRecords;
        this.dictionarySize = dictionarySize;
        this.repetitionWindow = repetitionWindow;
        this.compressionThresholdPercentage = compressionThresholdPercentage;

        this.isEnabled = true;
    }

    public OptimizationConfig() {
        this.isEnabled = false;
    }

    public boolean isCompressionEnabled(){
        return isEnabled;
    }

    public void enableCompression(){
        if (timeoutDuringCompression > 0
                && analyzeEveryNRecords > 0
                && dictionarySize > 0
                && repetitionWindow > 0
                && compressionThresholdPercentage > 0
                && compressionThresholdPercentage < 100) {
            this.isEnabled = true;
        }
        else {
            throw new IllegalStateException("Compression can not be enabled: Illegal configuration parameter! ");
        }

    }
    public int getTimeoutDuringCompression() {
        return timeoutDuringCompression;
    }

    public void setTimeoutDuringCompression(int timeoutDuringCompression) {
        this.timeoutDuringCompression = timeoutDuringCompression;
    }

    public int getAnalyzeEveryNRecords() {
        return analyzeEveryNRecords;
    }

    public void setAnalyzeEveryNRecords(int analyzeEveryNRecords) {
        this.analyzeEveryNRecords = analyzeEveryNRecords;
    }

    public int getDictionarySize() {
        return dictionarySize;
    }

    public void setDictionarySize(int dictionarySize) {
        this.dictionarySize = dictionarySize;
    }

    public int getRepetitionWindow() {
        return repetitionWindow;
    }

    public void setRepetitionWindow(int repetitionWindow) {
        this.repetitionWindow = repetitionWindow;
    }

    public double getCompressionThresholdPercentage() {
        return compressionThresholdPercentage;
    }

    public void setCompressionThresholdPercentage(int compressionThresholdPercentage) {
        this.compressionThresholdPercentage = compressionThresholdPercentage/100.0;
    }


}
