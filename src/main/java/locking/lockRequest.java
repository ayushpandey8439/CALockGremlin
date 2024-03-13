package locking;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class lockRequest {
    List<Object> criticalAncestors;
    Set<Object> Id;
    int mode; // 0 for local read, 1 for local write and 2 for global read. There is never a global write.
    int Oseq;
    public final Object accessController = new Object();
    public Boolean locked = true;

    public lockRequest(Set<Object> Id, int mode, List<Object> criticalAncestors) {
        this.Id = Id;
        this.mode = mode;
        this.Oseq = -1;
        this.criticalAncestors = criticalAncestors;
    }
}


