package org.apache.flink.streaming.runtime.optimization;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * Created by tobiasmuench on 28.12.18.
 */
public class CompressedStreamRecord extends StreamElement{

	public final long compressedValue;
	public long timestamp;
	public final boolean hasTimestamp;

	public CompressedStreamRecord(long timestamp, long hashComprRec) {
		this.compressedValue = hashComprRec;
		this.timestamp = timestamp;
		this.hasTimestamp = true;
	}

	public CompressedStreamRecord(long hashComprRec) {
		this.compressedValue = hashComprRec;
		this.hasTimestamp = false;
	}

	@Override
	public String toString(){
		return "CompressedStreamRecord@" + (hasTimestamp ? timestamp : "(noTS)") + " : CompressionKey = " + compressedValue;
	}
}
