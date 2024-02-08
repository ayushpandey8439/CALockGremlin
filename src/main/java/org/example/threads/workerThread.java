package org.example.threads;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.example.operations.Operation;
import org.example.operations.Queries;

import java.util.Random;

public class workerThread extends Queries implements Runnable {
    TinkerGraph graph;
    public workerThread(TinkerGraph graph){
        this.graph = graph;
    }
    @Override
    public void run() {
        runQuery();
    }

    public void runQuery(){
        double randomNumber = Math.random();
        // Print the random number
        for (int i = 0; i < Operation.queryDistribution.size(); i++) {
            if(randomNumber <= Operation.queryDistribution.get(i)) {
                System.out.println("Running query: "+Operation.queries.get(Operation.queryDistribution.get(i)).getLeft());
                //Operation.queries.get(Operation.queryDistribution.get(i)).accept(new Object[]{graph});
                break;
            }
        }
    }
}
