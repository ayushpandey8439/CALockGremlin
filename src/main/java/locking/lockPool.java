package locking;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.Main.config;

public class lockPool {
    final int NUM_THREADS = config.getInt("numThreads");
    lockRequest[] locks = new lockRequest[NUM_THREADS];
    Semaphore[] semaphores = new Semaphore[NUM_THREADS];
    AtomicInteger Oseq = new AtomicInteger(0);
    lockPool(){
        for(int i=0;i<NUM_THREADS;i++){
            semaphores[i] = new Semaphore(1);
            locks[i] = null;
        }
    }


    boolean lock(lockRequest reqObj, int threadId){
        synchronized (locks) {
            reqObj.Oseq = Oseq.getAndIncrement();
            locks[threadId] = reqObj;
        }

        for(int i=0;i<NUM_THREADS;i++){
            lockRequest l = locks[i];
            while(l!=null &&
                    (reqObj.mode==1 || (reqObj.mode==0 && l.mode == 1)) &&
                    (l.criticalAncestors.contains(reqObj.Id) || reqObj.criticalAncestors.contains(l.Id)) &&
                    (reqObj.Oseq > l.Oseq)){
                if(Runtime.getRuntime().availableProcessors() < NUM_THREADS){
                    Thread.yield();
                }
                l = locks[i];
            }
        }
        return true;
    }

    boolean unlock(int threadId){
        synchronized (locks) {
            locks[threadId] = null;
        }
        return true;
    }

}
