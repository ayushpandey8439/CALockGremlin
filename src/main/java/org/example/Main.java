package org.example;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.example.operations.Operation;
import org.example.operations.Queries;

import java.io.IOException;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    static TinkerGraph graph = TinkerGraph.open();
    public static void main(String[] args) throws IOException {
        TinkerGraph graph = TinkerGraph.open(); //1
        graph.loadGraphSON("/home/pandey/work/CALockGremlin/graphs/yeast.json");
        System.out.println("Loaded graph. Initiating labelling...");
        System.out.println("Number of roots: "+graph.roots.size());
        System.out.println("Number of sinks: "+graph.sinks.size());
        graph.labelGraph();
        System.out.println("Labelling complete. Initiating thread pool...");
        runBenchmark();
    }


    public static void runBenchmark(){
        Operation operation = new Operation();
        operation.computeQueryMix();

//        ExecutorService threadPool= newFixedThreadPool(1);
//        for(int i=0;i<5000;i++){
//            threadPool.submit(new workerThread(graph, queries));
//        }
//        threadPool.shutdown();

    }

}