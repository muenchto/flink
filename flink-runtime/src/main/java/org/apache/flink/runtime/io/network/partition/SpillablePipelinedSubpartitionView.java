package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * View over a pipelined in-memory only subpartition, that has not spilled anything.
 * Copy of {@link PipelinedSubpartitionView}.
 */
public class SpillablePipelinedSubpartitionView implements ResultSubpartitionView {

	/** The subpartition this view belongs to. */
	private final SpillablePipelinedSubpartition subpartition;

	private final BufferAvailabilityListener availabilityListener;

	/** Flag indicating whether this view has been released. */
	private final AtomicBoolean isReleased;

	SpillablePipelinedSubpartitionView(
		SpillablePipelinedSubpartition subpartition,
		int queueSize,
		BufferAvailabilityListener listener) throws IOException {

		this.subpartition = checkNotNull(subpartition);
		this.availabilityListener = checkNotNull(listener);
		this.isReleased = new AtomicBoolean();

		this.availabilityListener.notifyBuffersAvailable(queueSize);
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {
		return subpartition.pollBuffer();
	}

	@Override
	public void notifyBuffersAvailable(long numBuffers) throws IOException {
		availabilityListener.notifyBuffersAvailable(numBuffers);
	}

	@Override
	public void notifySubpartitionConsumed() {
		releaseAllResources();
	}

	@Override
	public void releaseAllResources() {
		if (isReleased.compareAndSet(false, true)) {
			// The view doesn't hold any resources and the parent cannot be restarted. Therefore,
			// it's OK to notify about consumption as well.
			subpartition.onConsumedSubpartition();
		}
	}

	@Override
	public boolean isReleased() {
		return isReleased.get() || subpartition.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartition.getFailureCause();
	}

	@Override
	public String toString() {
		return String.format("PipelinedSubpartitionView(index: %d) of ResultPartition %s",
			subpartition.index,
			subpartition.parent.getPartitionId());
	}
}
