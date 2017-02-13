package util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reusable utility class that provides synchronization mechanism similar to row-level DB locking.
 * <p>
 * The class is supposed to be used by the components that are responsible for managing
 * storage and caching of different type of entities in the application.
 * EntityLocker itself does not deal with the entities, only with the IDs (primary keys) of the entities.
 */
public class EntityLocker<T extends Comparable<T>> {
    private ConcurrentMap<T, ReentrantLock> lockByID = new ConcurrentHashMap<>();
    /**
     * Max locked ID (for deadlock prevention)
     */
    private ThreadLocal<T> maxLockedId = new ThreadLocal<>();

    /**
     * Run with lock by ID
     *
     * @param ID Entity ID
     */
    public void withLock(T ID, Runnable protectedCode) throws InterruptedException {
        tryWithLock(ID, -1, protectedCode);
    }

    /**
     * @param ID            Entity ID
     * @param timeOut       Time out for lock, -1 - no timeout
     * @param protectedCode Protected code to run
     */
    public void tryWithLock(T ID, int timeOut, Runnable protectedCode) throws InterruptedException {
        // Deadlock prevention
        T prevID = maxLockedId.get();
        if (prevID != null && ID.compareTo(prevID) < 0) {
            throw new InterruptedException("Deadlock prevented: " + prevID + " > " + ID);
        }

        // Create lock
        Lock lock = getLock(ID);
        boolean locked;
        if (timeOut == -1) {
            lock.lock();
            locked = true;
        } else {
            locked = lock.tryLock(timeOut, TimeUnit.MILLISECONDS);
        }

        // If successful
        if (locked) {
            maxLockedId.set(ID);
            try {
                // Run protected code
                protectedCode.run();
            } finally {
                lock.unlock();
                maxLockedId.set(prevID);
            }
        }
    }

    private Lock getLock(T ID) {
        return lockByID.computeIfAbsent(ID, k -> new ReentrantLock());
    }
}
