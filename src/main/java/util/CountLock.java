package util;

import java.util.concurrent.locks.ReentrantLock;

public class CountLock extends ReentrantLock {
    private volatile int enters = 0;

    int getEnters() {
        return enters;
    }

    public void lock() {
        enters++;
        super.lock();
    }

    public void unlock() {
        enters--;
        super.unlock();
    }
}
