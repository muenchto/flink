package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.*;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SpillablePipelinedSubpartitionTest extends SubpartitionTestBase {

	/** Executor service for concurrent produce/consume tests */
	private final static ExecutorService executorService = Executors.newCachedThreadPool();

	private static final IOManager ioManager = new IOManagerAsync();

	@AfterClass
	public static void shutdownExecutorService() throws Exception {
		executorService.shutdownNow();
	}

	@Override
	SpillablePipelinedSubpartition createSubpartition() {
		final ResultPartition parent = mock(ResultPartition.class);
		BufferProvider bufferProvider = mock(BufferProvider.class);
		when(parent.getBufferProvider()).thenReturn(bufferProvider);
		when(bufferProvider.getMemorySegmentSize()).thenReturn(32 * 1024);
		return new SpillablePipelinedSubpartition(0, parent, ioManager);
	}

	@Test
	public void testBasicPipelinedProduceConsumeLogic() throws Exception {
		final SpillablePipelinedSubpartition subpartition = createSubpartition();

		BufferAvailabilityListener listener = mock(BufferAvailabilityListener.class);

		ResultSubpartitionView view = subpartition.createReadView(listener);

		// Empty => should return null
		assertNull(view.getNextBuffer());
		verify(listener, times(1)).notifyBuffersAvailable(eq(0L));

		// Add data to the queue...
		subpartition.add(createBuffer());

		// ...should have resulted in a notification
		verify(listener, times(1)).notifyBuffersAvailable(eq(1L));

		// ...and one available result
		assertNotNull(view.getNextBuffer());
		assertNull(view.getNextBuffer());

		// Add data to the queue...
		subpartition.add(createBuffer());
		verify(listener, times(2)).notifyBuffersAvailable(eq(1L));
	}

	@Test
	public void testConcurrentFastProduceAndFastConsume() throws Exception {
		testProduceConsume(false, false);
	}

	@Test
	public void testConcurrentFastProduceAndSlowConsume() throws Exception {
		testProduceConsume(false, true);
	}

	@Test
	public void testConcurrentSlowProduceAndFastConsume() throws Exception {
		testProduceConsume(true, false);
	}

	@Test
	public void testConcurrentSlowProduceAndSlowConsume() throws Exception {
		testProduceConsume(true, true);
	}

	/**
	 * Verifies that the isReleased() check of the view checks the parent
	 * subpartition.
	 */
	@Test
	public void testIsReleasedChecksParent() throws Exception {
		PipelinedSubpartition subpartition = mock(PipelinedSubpartition.class);

		PipelinedSubpartitionView reader = new PipelinedSubpartitionView(
			subpartition, mock(BufferAvailabilityListener.class));

		assertFalse(reader.isReleased());
		verify(subpartition, times(1)).isReleased();

		when(subpartition.isReleased()).thenReturn(true);
		assertTrue(reader.isReleased());
		verify(subpartition, times(2)).isReleased();
	}

	private void testProduceConsume(boolean isSlowProducer, boolean isSlowConsumer) throws Exception {
		// Config
		final int producerBufferPoolSize = 8;
		final int producerNumberOfBuffersToProduce = 128;

		// Producer behaviour
		final TestProducerSource producerSource = new TestProducer(producerBufferPoolSize, producerNumberOfBuffersToProduce);

		// Consumer behaviour
		final TestConsumerCallback consumerCallback = new TestConsumer();

		final SpillablePipelinedSubpartition subpartition = createSubpartition();

		TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(isSlowConsumer, consumerCallback);
		final ResultSubpartitionView view = subpartition.createReadView(consumer);
		consumer.setSubpartitionView(view);

		Future<Boolean> producerResult = executorService.submit(
			new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource));

		Future<Boolean> consumerResult = executorService.submit(consumer);

		// Wait for producer and consumer to finish
		producerResult.get();
		consumerResult.get();
	}

	@Test
	public void testConcurrentFastProduceAndFastConsumeSpilling() throws Exception {
		testProduceConsumeWithSpilling(false, false);
	}

	@Test
	public void testConcurrentFastProduceAndSlowConsumeSpilling() throws Exception {
		testProduceConsumeWithSpilling(false, true);
	}

	@Test
	public void testConcurrentSlowProduceAndFastConsumeSpilling() throws Exception {
		testProduceConsumeWithSpilling(true, false);
	}

	@Test
	public void testConcurrentSlowProduceAndSlowConsumeSpilling() throws Exception {
		testProduceConsumeWithSpilling(true, true);
	}


	private void testProduceConsumeWithSpilling(boolean isSlowProducer, boolean isSlowConsumer) throws Exception {
		// Config
		final int producerBufferPoolSize = 8;
		final int producerNumberOfBuffersToProduce = 128;

		// Producer behaviour
		TestProducerSource producerSource = new TestProducer(producerBufferPoolSize, producerNumberOfBuffersToProduce);

		// Consumer behaviour
		final TestConsumerCallback consumerCallback = new TestConsumer();

		final SpillablePipelinedSubpartition subpartition = createSubpartition();

		TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(isSlowConsumer, consumerCallback);
		ResultSubpartitionView view = subpartition.createReadView(consumer);
		consumer.setSubpartitionView(view);

		Future<Boolean> producerResult = executorService.submit(new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource, false));
		Future<Boolean> consumerResult = executorService.submit(consumer);

		System.out.println("Starting normal InMemory-Phase");

		// Wait for producer and consumer to finish
		producerResult.get();
		System.out.println("Producer of InMemory-Phase done");
		subpartition.spillToDisk();

		consumerResult.get();

		System.out.println("Consumed all buffers from InMemory-Phase. Spilling to disk now.");

		producerSource = new TestProducer(producerBufferPoolSize, producerNumberOfBuffersToProduce);
		producerResult = executorService.submit(new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource, false));

		System.out.println("Submitted producer for SpillingToDisk-Phase");

		producerResult.get();

		System.out.println("Producer for SpillingToDisk-Phase complete");

		final TestConsumerCallback firstSpillableConsumerCallback = new TestConsumer();
		consumer = new TestSubpartitionConsumer(isSlowConsumer, firstSpillableConsumerCallback);
		view = subpartition.createReadView(consumer);
		consumer.setSubpartitionView(view);

		System.out.println("Entering last stage, where Consumer first consumes spilled and then memory buffers");

		// Magic Number for second producer
		producerSource = new TestProducer(producerBufferPoolSize, producerNumberOfBuffersToProduce, 1048576);
		consumerResult = executorService.submit(consumer);
		producerResult = executorService.submit(new TestSubpartitionProducer(subpartition, isSlowProducer, producerSource, false));

		System.out.println("Waiting for new memory producer and consumer to finish");

		// Wait for producer and consumer to finish
		producerResult.get();
		subpartition.add(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE));
		consumerResult.get();

		assertTrue(subpartition.isEmpty());
	}

	private class TestProducer implements TestProducerSource {

		private final int producerNumberOfBuffersToProduce;

		private final BufferProvider bufferProvider;
		private final int startingBase;

		private int numberOfBuffers;

		TestProducer(int bufferPoolSize, int producerNumberOfBuffersToProduce) {
			this(bufferPoolSize, producerNumberOfBuffersToProduce, 0);
		}

		TestProducer(int bufferPoolSize, int producerNumberOfBuffersToProduce, int startingBase) {
			this.producerNumberOfBuffersToProduce = producerNumberOfBuffersToProduce;
			this.bufferProvider = new TestPooledBufferProvider(bufferPoolSize);
			this.startingBase = startingBase;
		}

		@Override
		public BufferOrEvent getNextBufferOrEvent() throws Exception {
			if (numberOfBuffers == producerNumberOfBuffersToProduce) {
				return null;
			}

			final Buffer buffer = bufferProvider.requestBufferBlocking();

			final MemorySegment segment = buffer.getMemorySegment();

			int next = startingBase + numberOfBuffers * (segment.size() / 4);

			for (int i = 0; i < segment.size(); i += 4) {
				segment.putInt(i, next);

				next++;
			}

			numberOfBuffers++;

			return new BufferOrEvent(buffer, 0);
		}
	}

	private class TestConsumer implements TestConsumerCallback {

		private int numberOfBuffers;

		@Override
		public void onBuffer(Buffer buffer) {
			final MemorySegment segment = buffer.getMemorySegment();

			int expected = numberOfBuffers * (segment.size() / 4);

			for (int i = 0; i < segment.size(); i += 4) {
				assertEquals(expected, segment.getInt(i));

				expected++;
			}

			numberOfBuffers++;

			buffer.recycle();
		}

		@Override
		public void onEvent(AbstractEvent event) {
			// Nothing to do in this test
		}
	};
}
