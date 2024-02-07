package org.example.threads;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.example.operations.Queries;

import java.util.Random;

public class workerThread extends Queries implements Runnable {
    TinkerGraph graph;
    Queries queries;
    public workerThread(TinkerGraph graph, Queries queries){
        this.graph = graph;
        this.queries = queries;
    }
    @Override
    public void run() {
        runQuery();
    }

    public void runQuery(){
        //select a random number between 2 and 35
        Random random = new Random();

        // Generate a random number between 1 (inclusive) and 31 (exclusive)
        int randomNumber = random.nextInt(1,31);

        // Print the random number
        System.out.println(randomNumber);
    }
}
