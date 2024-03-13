package locking;

import org.apache.commons.lang3.time.StopWatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.example.Main.config;

public class lockPool {
    final int NUM_THREADS = config.getInt("threads");
    public lockRequest[] locks = new lockRequest[NUM_THREADS];
    Object[] threadNotifications = new Object[NUM_THREADS];

    AtomicInteger Oseq = new AtomicInteger(1);
    public final Object PoolLock = new Object();
    public final ReadWriteLock lock = new ReentrantReadWriteLock();
    public long latency = 0;   // in milliseconds


    public lockPool() {
        Arrays.fill(this.locks, null);
        Arrays.fill(this.threadNotifications, false);
    }


    public void lock(lockRequest reqObj, int threadId) {
        long start = System.currentTimeMillis();
        if (config.getString("lockType").equals("coarse")) {

            if (reqObj.mode == 1) {
                lock.writeLock().lock();
            } else {
                lock.readLock().lock();
            }

            this.locks[threadId] = reqObj;

        } else if (config.getString("lockType").equals("ca")) {
            synchronized (PoolLock) {
                reqObj.Oseq = Oseq.getAndIncrement();
                locks[threadId] = reqObj;
            }
            lockRequest l;
            for (int i = 0; i < NUM_THREADS; i++) {
                l = locks[i];
                while ((l != null && l.locked) &&
                        (i != threadId) &&
                        (((reqObj.mode | l.mode) == 3) || // one of them holds a global read lock and the other wants a write lock.
                                (!Collections.disjoint(reqObj.Id, l.Id) || !Collections.disjoint(l.criticalAncestors, reqObj.Id) || !Collections.disjoint(reqObj.criticalAncestors, l.Id))) &&
                        (reqObj.Oseq > l.Oseq)
                ) {
                    try {
                        synchronized (l.accessController) {
                            l.accessController.wait(100);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
//                    e.printStackTrace();
                    }
                    l = locks[i];
                }
            }
        }

//
//
        long end = System.currentTimeMillis();
        latency += end - start;
    }

    public void unlock(int threadId) {
        if (config.getString("lockType").equals("coarse")) {
            if (locks[threadId].mode == 1) {
                lock.writeLock().unlock();
            } else {
                lock.readLock().unlock();
            }
            this.locks[threadId] = null;
        } else if (config.getString("lockType").equals("ca")) {
            locks[threadId].locked = false;
            synchronized (PoolLock) {
                synchronized (locks[threadId].accessController) {
                    locks[threadId].accessController.notifyAll();
                }
                locks[threadId] = null;
            }
        }
//
    }

}
