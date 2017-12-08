/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.iomanager.BufferFileWriter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.SynchronousBufferFileReader;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.SpillToDiskMarker;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory subpartition, which can temporarily also spill to disk.
 */
public class SpillablePipelinedSubpartition extends ResultSubpartition {

	private static final Logger LOG = LoggerFactory.getLogger(SpillablePipelinedSubpartition.class);

	// ------------------------------------------------------------------------

	/** The I/O manager used for spilling buffers to disk. */
	private final IOManager ioManager;

	/** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
	private final ArrayDeque<Buffer> buffers = new ArrayDeque<>();

	/** The read view to consume this subpartition. */
	private ResultSubpartitionView readView;

	/** Flag indicating whether the subpartition has been finished. */
	private boolean isFinished;

	/** Flag indicating whether the subpartition has been released. */
	private volatile boolean isReleased;

	/** Flag indicating whether the subpartition is currently spilling to disk. */
	private volatile boolean isSpilling = false;

	private BufferFileWriter spillWriter;

	private final AtomicLong numberOfSpilledBuffers = new AtomicLong();

	// ------------------------------------------------------------------------

	private final String owningTaskName;

	public SpillablePipelinedSubpartition(int index, ResultPartition resultPartition, IOManager ioManager) {
		super(index, resultPartition);
		this.owningTaskName = resultPartition.owningTaskName;
		this.ioManager = checkNotNull(ioManager);
	}

	@Override
	public boolean add(Buffer buffer) throws IOException {
		checkNotNull(buffer);

		// view reference accessible outside the lock, but assigned inside the locked scope
		ResultSubpartitionView reader = null;
		boolean synchronizedIsSpilling = false;

		synchronized (buffers) {
			if (isFinished || isReleased) {
				return false;
			}

			LOG.info("For {} adding buffer to {} with spilling {}.", owningTaskName, buffers, isSpilling);

			updateStatistics(buffer);

			if (!isSpilling) {
				// Add the buffer and update the stats
				buffers.add(buffer);
				reader = readView;
			} else {
				synchronizedIsSpilling = true;
			}
		}

		if (synchronizedIsSpilling) {
			spillWriter.writeBlock(buffer);
			numberOfSpilledBuffers.incrementAndGet();
		}

		// Notify the listener outside of the synchronized block
		if (reader != null) {
			LOG.info("{} notifying {}.", owningTaskName, reader);
			reader.notifyBuffersAvailable(1);
		}

		return true;
	}

	@Override
	public void finish() throws IOException {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final ResultSubpartitionView reader;

		final Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE);

		synchronized (buffers) {
			if (isFinished || isReleased) {
				return;
			}

			buffers.add(buffer);
			reader = readView;
			updateStatistics(buffer);

			isFinished = true;
		}

		// If we are spilling/have spilled, wait for the writer to finish
		if (spillWriter != null) {
			spillWriter.close();
		}

		LOG.debug("Finished {}.", this);

		// Notify the listener outside of the synchronized block
		if (reader != null) {
			reader.notifyBuffersAvailable(1);
		}
	}

	public void spillToDisk() throws IOException {
		final Buffer buffer = EventSerializer.toBuffer(SpillToDiskMarker.INSTANCE);

		isSpilling = true;

		LOG.debug("Trying to start spilling subpartition {} for task {} to disk.", this, parent.owningTaskName);

		// view reference accessible outside the lock, but assigned inside the locked scope
		final ResultSubpartitionView reader;

		synchronized (buffers) {
			buffers.add(buffer);
			reader = readView;
			updateStatistics(buffer);

			this.spillWriter = ioManager.createBufferFileWriter(ioManager.createChannel());
		}

		LOG.debug("Start spilling subpartition {} for task {} to disk.", this, parent.owningTaskName);

		// Notify the listener outside of the synchronized block
		if (reader != null) {
			reader.notifyBuffersAvailable(1);
		}

		// From now on, we wait for a new readView to register
		readView = null;
	}

	@Override
	public void release() throws IOException {
		// view reference accessible outside the lock, but assigned inside the locked scope
		final ResultSubpartitionView view;

		synchronized (buffers) {
			if (isReleased) {
				return;
			}

			// Release all available buffers
			Buffer buffer;
			while ((buffer = buffers.poll()) != null) {
				buffer.recycle();
			}

			// Get the view...
			view = readView;

			// No consumer yet, we are responsible to clean everything up. If
			// one is available, the view is responsible is to clean up (see
			// below).
			if (view == null) {
				buffers.clear();

				// TODO This can block until all buffers are written out to
				// disk if a spill is in-progress before deleting the file.
				// It is possibly called from the Netty event loop threads,
				// which can bring down the network.
				if (spillWriter != null) {
					spillWriter.closeAndDelete();
				}
			}
			readView = null;

			// Make sure that no further buffers are added to the subpartition
			isReleased = true;
		}

		LOG.debug("Released {}.", this);

		// Release all resources of the view
		if (view != null) {
			view.releaseAllResources();
		}
	}

	Buffer pollBuffer() throws InterruptedException, IOException {
		synchronized (buffers) {
			return buffers.pollFirst();
		}
	}

	@Override
	public int releaseMemory() throws IOException {

		synchronized (buffers) {

			if (!isSpilling) {
				return 0;
			} else if (spillWriter != null) {
				// No view and in-memory => spill to disk

				int numberOfBuffers = buffers.size();
				long spilledBytes = 0;

				// Spill all buffers
				for (int i = 0; i < numberOfBuffers; i++) {
					Buffer buffer = buffers.remove();
					spilledBytes += buffer.getSize();
					spillWriter.writeBlock(buffer);
				}

				LOG.debug("Spilling {} bytes for sub partition {} of {}.", spilledBytes, index, parent.getPartitionId());

				return numberOfBuffers;
			}
		}

		// Else: We have already spilled and don't hold any buffers
		return 0;
	}

	@Override
	public boolean isReleased() {
		return isReleased;
	}

	@Override
	public ResultSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) throws IOException {
		final int queueSize;

		synchronized (buffers) {
			checkState(!isReleased);

			LOG.debug("Creating read view for subpartition {} of partition {} during spilling {}.",
				index, parent.getPartitionId(), isSpilling);

			queueSize = buffers.size();

			if (spillWriter == null) {
				readView = new SpillablePipelinedSubpartitionView(this, queueSize, availabilityListener);
			} else {
				readView = new SpilledPipelinedSubpartitionView(
					this,
					parent.getBufferProvider().getMemorySegmentSize(),
					spillWriter,
					numberOfSpilledBuffers.get(),
					availabilityListener);

				spillWriter = null;
			}

			isSpilling = false;
		}

		return readView;
	}

	// ------------------------------------------------------------------------

	int getCurrentNumberOfBuffers() {
		return buffers.size();
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final long numBuffers;
		final long numBytes;
		final boolean finished;
		final boolean hasReadView;

		synchronized (buffers) {
			numBuffers = getTotalNumberOfBuffers();
			numBytes = getTotalNumberOfBytes();
			finished = isFinished;
			hasReadView = readView != null;
		}

		return String.format(
				"PipelinedSubpartition for %s [number of buffers: %d (%d bytes), finished? %s, read view? %s]",
			owningTaskName, numBuffers, numBytes, finished, hasReadView);
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		// since we do not synchronize, the size may actually be lower than 0!
		return Math.max(buffers.size(), 0);
	}

	public boolean isEmpty() {
		synchronized (buffers) {
			return buffers.isEmpty();
		}
	}
}
