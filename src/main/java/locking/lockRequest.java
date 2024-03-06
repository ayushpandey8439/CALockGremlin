package locking;

import java.util.List;
import java.util.Set;

public class lockRequest {
    List<Object> criticalAncestors;
    Set<Object> Id;
    int mode; // 0 for local read, 1 for local write and 2 for global read. There is never a global write.
    int Oseq;

    public lockRequest(Set<Object> Id, int mode, List<Object> criticalAncestors) {
        this.Id = Id;
        this.mode = mode;
        this.Oseq = -1;
        this.criticalAncestors = criticalAncestors;
    }
}


