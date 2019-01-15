package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * Created by tobiasmuench on 30.12.18.
 */
public class CompressionMarker extends StreamElement {

    private boolean isEnabler;

    public CompressionMarker asEnabler() {
        isEnabler = true;
        return this;
    }

    public CompressionMarker asDisabler() {
        isEnabler = false;
        return this;
    }

    public boolean isEnabler() {
        return isEnabler;
    }

    @Override
    public String toString(){
        if (isEnabler){
            return "CompressionMarker(Enabler)";
        }
        else {
            return "CompressionMarker(Disabler)";
        }
    }
}
