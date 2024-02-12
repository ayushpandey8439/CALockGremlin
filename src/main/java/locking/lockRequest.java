package locking;

import java.util.Set;

public class lockRequest {
    Set<Integer> criticalAncestors;
    int Id;
    int mode;
    int Oseq;

    lockRequest(int Id, int mode, int Oseq, Set<Integer> criticalAncestors){
        this.Id = Id;
        this.mode = mode;
        this.Oseq = -1;
        this.criticalAncestors = criticalAncestors;
    }
}


