package by.lobanov.learntocodejavacore.concurrency.ringbuffer;

import lombok.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LockRingBufferTest {

    private RingBuffer<String> ringBuffer;
    private final int baseCapacity = 7;
    private ProducerRingBuffer<String> producer;
    private ConsumerRingBuffer<String> consumer;

    @Test
    void givenArray_whenRunProducer_thenSizeShouldBeTheSame() throws InterruptedException {
        // given
        ringBuffer = new LockRingBuffer<>(baseCapacity);
        String[] items = {"Java", "Kotlin", "Groovy", "Scala"};
        producer = new ProducerRingBuffer<>(items, ringBuffer);

        // when
        Thread thread = new Thread(producer);
        thread.start();
        thread.join();

        // then
        Assertions.assertEquals(items.length, ringBuffer.size(), "Buffer size should match number of puts");
    }

    @ParameterizedTest()
    @CsvSource(value = {"1:1", "2:2", "3:3"}, delimiter = ':')
    @SneakyThrows
    void givenSourceData_whenOfferAndTake_thenShouldBeEqual(String putValue, String expectPopValue) {
        ringBuffer = new LockRingBuffer<>(baseCapacity);
        ringBuffer.offer(putValue);
        String popValue = ringBuffer.take();
        Assertions.assertEquals(expectPopValue, popValue);
    }

    @Test
    void givenBuffer_whenCallIsEmpty_thenShouldReturnTrue() {
        ringBuffer = new LockRingBuffer<>(baseCapacity);
        assertTrue(ringBuffer.isEmpty(), "Buffer should be empty");
    }

    @Test
    void givenBuffer_whenOfferAndCallIsEmpty_thenShouldReturnFalse() {
        // given
        ringBuffer = new LockRingBuffer<>(baseCapacity);

        // when
        ringBuffer.offer("test");

        // then
        Assertions.assertFalse(ringBuffer.isEmpty(), "Buffer should not be empty");
    }

    @Test
    void givenBuffer_whenOfferElementsEqualCapacityAndCallIsFull_thenShouldReturnTrue() {
        // given
        ringBuffer = new LockRingBuffer<>(baseCapacity);

        // when
        for (int i = 0; i < baseCapacity; i++) {
            ringBuffer.offer("test");
        }

        // then
        assertTrue(ringBuffer.isFull(), "Buffer should be full");
    }

    @Test
    @Timeout(15)
    void givenBuffer_whenConsumerAndProducerWorks_thenConsumerShouldReturnThatProducerOffer() throws InterruptedException {
        // given
        ringBuffer = new LockRingBuffer<>(baseCapacity);
        String[] items = {"Java", "Kotlin", "Groovy", "Scala"};
        producer = new ProducerRingBuffer<>(items, ringBuffer);


        // when
        Thread threadProducer = new Thread(producer);
        threadProducer.start();

        Thread threadConsumer = new Thread(() -> {
            try {
                threadProducer.join();
                consumer = new ConsumerRingBuffer<>(ringBuffer);
                consumer.run();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        threadConsumer.start();
        threadConsumer.join();

        // then
        Assertions.assertEquals(0, ringBuffer.size(), "Buffer size zero after take");
        Assertions.assertTrue(ringBuffer.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7})
    void givenBufferAndProducer_whenOfferArrayOfElements_thenSizeShouldBeEquals(int capacity) throws InterruptedException {
        // given
        ringBuffer = new LockRingBuffer<>(capacity);
        String[] items = IntStream.rangeClosed(1, capacity).mapToObj(String::valueOf).toArray(String[]::new);
        producer = new ProducerRingBuffer<>(items, ringBuffer);

        // when
        Thread thread = new Thread(producer);
        thread.start();
        thread.join();

        // then
        Assertions.assertEquals(items.length, ringBuffer.size(), "Buffer size should match number of puts");
    }

    @Test
    @Timeout(10)
    void givenFullBuffer_whenProducerWaitsAndConsumerTakes_thenProducerShouldProceed() throws InterruptedException {
        // given
        int smallCapacity = 2;
        ringBuffer = new LockRingBuffer<>(smallCapacity);
        ringBuffer.offer("Java");
        ringBuffer.offer("Kotlin");
        CountDownLatch producerStarted = new CountDownLatch(1);
        CountDownLatch consumerStarted = new CountDownLatch(1);
        AtomicBoolean producerBlocked = new AtomicBoolean(false);
        AtomicBoolean isProducerWasBlocked = new AtomicBoolean(false);


        // when
        Thread producerThread = new Thread(() -> {
            try {
                producerStarted.countDown();
                consumerStarted.await();
                producerBlocked.set(true);
                ringBuffer.offer("Scala");
                producerBlocked.set(false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread consumerThread = new Thread(() -> {
            try {
                producerStarted.await();
                Thread.sleep(100);
                consumerStarted.countDown();
                Thread.sleep(100);
                isProducerWasBlocked.set(producerBlocked.get());
                ringBuffer.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();

        // then
        assertTrue(isProducerWasBlocked.get(), "Producer should be blocked");
        Assertions.assertEquals(2, ringBuffer.size(), "Buffer should have 2 elements after operations");
    }

    @Test
    @Timeout(15)
    void givenMultipleProducersAndConsumers_whenConcurrentAccess_thenShouldWorkCorrectly() throws InterruptedException {
        // given
        int capacity = 5;
        int itemsPerProducer = 10;
        int numberOfProducers = 3;
        int numberOfConsumers = 2;

        ringBuffer = new LockRingBuffer<>(capacity);
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfProducers + numberOfConsumers);
        CountDownLatch allTasksCompleted = new CountDownLatch(numberOfProducers + numberOfConsumers);
        ConcurrentLinkedQueue<String> consumedItems = new ConcurrentLinkedQueue<>();


        // when
        // Run producers
        for (int p = 0; p < numberOfProducers; p++) {
            final int producerId = p;
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < itemsPerProducer; i++) {
                        ringBuffer.offer("Producer" + producerId + "-Item" + i);
                    }
                } finally {
                    allTasksCompleted.countDown();
                }
            });
        }

        // Run consumers
        for (int c = 0; c < numberOfConsumers; c++) {
            executorService.submit(() -> {
                try {
                    int itemsToConsume = (numberOfProducers * itemsPerProducer) / numberOfConsumers;
                    for (int i = 0; i < itemsToConsume; i++) {
                        String item = ringBuffer.take();
                        consumedItems.offer(item);
                    }
                } finally {
                    allTasksCompleted.countDown();
                }
            });
        }

        allTasksCompleted.await();
        executorService.shutdown();

        // then
        int expectedTotalItems = numberOfProducers * itemsPerProducer;
        Assertions.assertEquals(expectedTotalItems, consumedItems.size(), "All produced items should be consumed");

        // Проверяем, что все элементы уникальны (нет дублирования из-за race conditions)
        long uniqueItems = consumedItems.stream().distinct().count();
        Assertions.assertEquals(expectedTotalItems, uniqueItems, "All consumed items should be unique");
    }

    @Test
    void givenNullObject_whenOffer_thenNPEIsThrown() {
        // given
        ringBuffer = new LockRingBuffer<>(10);
        String nullObject = null;

        // when
        Throwable throwable = Assertions.assertThrows(NullPointerException.class,
                () -> ringBuffer.offer(nullObject));

        // then
        Assertions.assertEquals(NullPointerException.class, throwable.getClass());
    }

    @Test
    void givenClosedAndFullBuffer_whenDoOffer_thenExceptionIsThrown() {
        // given
        ringBuffer = new LockRingBuffer<>(1);
        ringBuffer.close();
        ringBuffer.offer("first object");
        String newObject = "new object";

        // when
        Throwable throwable = Assertions.assertThrows(IllegalStateException.class,
                () -> ringBuffer.offer(newObject));

        // then
        Assertions.assertEquals(IllegalStateException.class, throwable.getClass());
    }

    @Test
    void givenClosedAndEmptyBuffer_whenDoTake_thenExceptionIsThrown() {
        // given
        ringBuffer = new LockRingBuffer<>(1);
        ringBuffer.close();

        // when
        Throwable throwable = Assertions.assertThrows(IllegalStateException.class,
                () -> ringBuffer.take());

        // then
        Assertions.assertEquals(IllegalStateException.class, throwable.getClass());
    }

    // Classes for test

    private static class ProducerRingBuffer<T> implements Runnable {
        private final T[] items;
        private final RingBuffer<T> ringBuffer;

        public ProducerRingBuffer(T[] items, RingBuffer<T> ringBuffer) {
            this.items = items;
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void run() {
            for (int i = 0; i < items.length; ) {
                if (this.ringBuffer.offer(items[i])) {
                    i++;
                    System.out.println(Thread.currentThread());
                }
            }
        }
    }

    private static class ConsumerRingBuffer<T> implements Runnable {

        private final int expectedCount;
        private final RingBuffer<T> ringBuffer;

        public ConsumerRingBuffer(RingBuffer<T> ringBuffer) {
            this.ringBuffer = ringBuffer;
            this.expectedCount = ringBuffer.size();
        }

        @SneakyThrows
        @Override
        public void run() {
            for (int i = 0; i < expectedCount; i++) {
                ringBuffer.take();
            }
        }
    }
}
