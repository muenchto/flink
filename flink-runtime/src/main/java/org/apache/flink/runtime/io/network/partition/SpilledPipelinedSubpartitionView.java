package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileReader;
import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * View over a pipelined in-memory only subpartition, that can also spill to disk.
 * If there are some buffers on disk, consume them first, and in-memory later.
 */
public class SpilledPipelinedSubpartitionView implements ResultSubpartitionView, NotificationListener {

	private static final Logger LOG = LoggerFactory.getLogger(SpilledPipelinedSubpartitionView.class);

	/** The subpartition this view belongs to.*/
	private final SpillablePipelinedSubpartition subpartition;

	private final BufferAvailabilityListener availabilityListener;

	/** Flag indicating whether this view has been released. */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/** Writer for spills. */
	private final BufferFileWriter spillWriter;

	/** The synchronous file reader to do the actual I/O. */
	private final BufferFileReader fileReader;

	/** The buffer pool to read data into. */
	private final SpilledSubpartitionView.SpillReadBufferPool bufferPool;

	/** The total number of spilled buffers */
	private AtomicLong numberOfSpilledBuffers;

	/** Flag indicating whether a spill is still in progress */
	private volatile boolean isSpillInProgress = true;

	SpilledPipelinedSubpartitionView(SpillablePipelinedSubpartition subpartition,
									 int memorySegmentSize,
									 BufferFileWriter spillWriter,
									 long numberOfSpilledBuffers,
									 BufferAvailabilityListener listener) throws IOException {
		this.subpartition = checkNotNull(subpartition);
		this.availabilityListener = checkNotNull(listener);

		this.bufferPool = new SpilledSubpartitionView.SpillReadBufferPool(8, memorySegmentSize);
		this.spillWriter = checkNotNull(spillWriter);
		this.fileReader = new SynchronousBufferFileReader(spillWriter.getChannelID(), false);
		checkArgument(numberOfSpilledBuffers >= 0);
		this.numberOfSpilledBuffers = new AtomicLong(numberOfSpilledBuffers);

		// Check whether async spilling is still in progress. If not, this returns
		// false and we can notify our availability listener about all available buffers.
		// Otherwise, we notify only when the spill writer callback happens.
		if (!spillWriter.registerAllRequestsProcessedListener(this)) {
			isSpillInProgress = false;
			availabilityListener.notifyBuffersAvailable(numberOfSpilledBuffers);
			LOG.debug("No spilling in progress. Notified about {} available buffers.", numberOfSpilledBuffers);
		} else {
			LOG.debug("Spilling in progress. Waiting with notification about {} available buffers.", numberOfSpilledBuffers);
		}
	}

	@Override
	public void notifyBuffersAvailable(long numBuffers) throws IOException {
		if (isSpillInProgress) {
			numberOfSpilledBuffers.addAndGet(numBuffers);
		} else {
			availabilityListener.notifyBuffersAvailable(numBuffers);
		}
	}

	/**
	 * This is the call back method for the spill writer. If a spill is still
	 * in progress when this view is created we wait until this method is called
	 * before we notify the availability listener.
	 */
	@Override
	public void onNotification() {
		isSpillInProgress = false;
		availabilityListener.notifyBuffersAvailable(numberOfSpilledBuffers.get());
		LOG.debug("Finished spilling. Notified about {} available buffers.", numberOfSpilledBuffers);
	}

	@Override
	public Buffer getNextBuffer() throws IOException, InterruptedException {

		// No locking needed, since we do not notifyForAvailableBuffers before?
		if (fileReader.getSize() > 0 && !fileReader.hasReachedEndOfFile()) {

			numberOfSpilledBuffers.decrementAndGet();

			// TODO This is fragile as we implicitly expect that multiple calls to
			// this method don't happen before recycling buffers returned earlier.
			Buffer buffer = bufferPool.requestBufferBlocking();
			fileReader.readInto(buffer);

			return buffer;
		} else {

			LOG.info("Finished Reading from fileReader and reading from subpartition {} again", subpartition);

			return subpartition.pollBuffer();
		}
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		subpartition.onConsumedSubpartition();
		releaseAllResources();
	}

	@Override
	public void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			// TODO This can block until all buffers are written out to
			// disk if a spill is in-progress before deleting the file.
			// It is possibly called from the Netty event loop threads,
			// which can bring down the network.
			spillWriter.closeAndDelete();

			fileReader.close();
			bufferPool.destroy();
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
		return String.format("SpilledPipelinedSubpartitionView(index: %d) of ResultPartition %s",
			subpartition.index,
			subpartition.parent.getPartitionId());
	}
}
