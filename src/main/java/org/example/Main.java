package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.javatuples.Pair;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    }


    public void setupThreadPool(){
        ExecutorService threadPool= newFixedThreadPool(1);
        for(int i=0;i<1000;i++){
            threadPool.submit(new workerThread(graph));
        }

    }

}