package org.example;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

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
        //select a random number between 2 and 35
        Random random = new Random();

        // Generate a random number between 2 (inclusive) and 35 (exclusive)
        int randomNumber = random.nextInt(34) + 2;

        // Print the random number
        System.out.println("Random Number between 2 and 35: " + randomNumber);
    }
}
