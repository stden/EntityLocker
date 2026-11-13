import org.junit.Assert;
import org.junit.Test;
import util.EntityLocker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EntityLockerTest extends Assert {

    private static final int THREADS = 1000;
    private static final int ITERATIONS = 1000;

    private int counter = 0;

    /**
     * EntityLocker should support different types of entity IDs
     */
    @Test
    public void testSupportDifferentTypes() throws InterruptedException {
        // Integer
        EntityLocker<Integer> locker1 = new EntityLocker<>();
        AtomicBoolean passed1 = new AtomicBoolean(false);
        locker1.withLock(1, () -> passed1.set(true));
        assertTrue(passed1.get());
        // String
        EntityLocker<String> locker2 = new EntityLocker<>();
        AtomicBoolean passed2 = new AtomicBoolean(false);
        locker2.withLock("This is test", () -> passed2.set(true));
        assertTrue(passed2.get());
    }

    @Test
    public void testModifyOneRowInMultiThreads() throws InterruptedException {
        EntityLocker<Integer> locker = new EntityLocker<>();
        int counterID = 1;
        counter = 0;
        Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    locker.withLock(counterID, () -> {
                        for (int j = 0; j < ITERATIONS; j++) {
                            counter++;
                        }
                    });
                } catch (InterruptedException e) {
                    fail();
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        assertEquals(THREADS * ITERATIONS, counter);
    }


    /**
     * Allow reentrant locking
     */
    @Test
    public void testReentrantLocking() throws InterruptedException {
        EntityLocker<Integer> locker = new EntityLocker<>();
        int counterID = 1;
        counter = 0;

        Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < ITERATIONS; j++) {
                    try {
                        locker.withLock(counterID, () -> {
                            counter++;
                            try {
                                locker.withLock(counterID, () -> counter++);
                            } catch (InterruptedException e) {
                                fail();
                            }
                        });
                    } catch (InterruptedException e) {
                        fail();
                    }
                }
            });
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        assertEquals(2 * THREADS * ITERATIONS, counter);
    }

    /**
     * Allow the caller to specify timeout for locking an entity.
     */
    @Test
    public void testTimeouts() throws InterruptedException {
        EntityLocker<Integer> locker = new EntityLocker<>();
        CountDownLatch entityLocked = new CountDownLatch(1);
        AtomicBoolean t1finished = new AtomicBoolean(false);

        // Lock entity from thread t1
        Thread t1 = new Thread(() -> {
            try {
                locker.tryWithLock(1, 50, () -> {
                    entityLocked.countDown();
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        fail();
                    }
                    t1finished.set(true);
                });
            } catch (InterruptedException e) {
                fail();
            }
        });
        t1.start();
        entityLocked.await();
        assertEquals(0, entityLocked.getCount());

        AtomicBoolean thread2 = new AtomicBoolean(false);
        locker.tryWithLock(1, 50, () -> thread2.set(true));
        t1.join();
        assertFalse(thread2.get());
        assertTrue(t1finished.get());
    }

    /**
     * Implement protection from deadlocks (but not taking into account possible locks outside EntityLocker)
     */
    @Test
    public void testDeadlockPrevention() throws InterruptedException {
        EntityLocker<Integer> entityLocker = new EntityLocker<>();

        // Thread #1: 1 2
        AtomicInteger counter12 = new AtomicInteger(0);
        Thread t1 = new Thread(() -> {
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    entityLocker.withLock(1, () -> {
                        try {
                            entityLocker.withLock(2, counter12::incrementAndGet);
                        } catch (InterruptedException ex) {
                            fail(ex.getMessage());
                        }
                    });
                }
            } catch (InterruptedException ex) {
                fail(ex.getMessage());
            }
        });

        // Thread #2: 2 1
        AtomicBoolean deadLockPrevented = new AtomicBoolean(false);
        AtomicBoolean innerCodeCalled = new AtomicBoolean(false);
        Thread t2 = new Thread(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                try {
                    entityLocker.withLock(2, () -> {
                        try {
                            entityLocker.withLock(1, () -> innerCodeCalled.set(true));
                        } catch (InterruptedException e) {
                            System.out.println(e.getMessage());
                            deadLockPrevented.set(true);
                        }
                    });
                } catch (InterruptedException ex) {
                    fail(ex.getMessage());
                }
            }
        });
        // Run and wait both threads
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        assertTrue("Deadlock prevented", deadLockPrevented.get());
        assertFalse("Inner code should not be called", innerCodeCalled.get());
        assertEquals(ITERATIONS, counter12.get());
    }
}
