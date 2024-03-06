package locking;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.example.Main.config;

public class lockPool {
    final int NUM_THREADS = config.getInt("threads");
    lockRequest[] locks = new lockRequest[NUM_THREADS];
    AtomicInteger Oseq = new AtomicInteger(1);
    final Object PoolLock = new Object();
    final ReadWriteLock lock = new ReentrantReadWriteLock();

    public lockPool() {
        Arrays.fill(this.locks, null);
    }


    public void lock(lockRequest reqObj, int threadId) {
//        if(reqObj.mode == 0) {
//            lock.readLock().lock();
//        } else {
//            lock.writeLock().lock();
//        }
        synchronized (PoolLock) {
            reqObj.Oseq = Oseq.getAndIncrement();
            this.locks[threadId] = reqObj;
        }
        lockRequest l;
        for (int i = 0; i < NUM_THREADS; i++) {
            l = this.locks[i];
            while ((l != null) &&
                    (i != threadId) &&
                    ((((reqObj.mode | l.mode) == 3)) || // one of them holds a global read lock and the other wants a write lock.
                            ((((reqObj.mode | l.mode) < 3)) && (!Collections.disjoint(reqObj.Id, l.Id) || !Collections.disjoint(l.criticalAncestors, reqObj.Id) || !Collections.disjoint(reqObj.criticalAncestors, l.Id)))) &&
                    (reqObj.Oseq > l.Oseq)
            ) {
                l = this.locks[i];
            }
        }
    }

    public void unlock(int threadId) {
//        if (this.locks[threadId].mode == 0) {
//            lock.readLock().unlock();
//        } else {
//            lock.writeLock().unlock();
//        }
        synchronized (PoolLock) {
            locks[threadId].Oseq = Integer.MAX_VALUE;
            this.locks[threadId] = null;
        }
    }

}
