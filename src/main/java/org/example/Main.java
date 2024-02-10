package org.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.example.operations.Operation;
import org.example.operations.Queries;
import org.example.threads.workerThread;
import org.javatuples.Pair;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    static TinkerGraph graph = TinkerGraph.open();
    public static void main(String[] args) throws IOException, CsvValidationException {
        TinkerGraph graph = TinkerGraph.open(); //1
        String filename = "yeast";

        // Use this to extract the snapshot and workload from the CSV files.
//        exrtractWorkloadFromCsv("/home/pandey/work/CALockGremlin/graphs/"+filename+".csv",
//                "/home/pandey/work/CALockGremlin/graphs/"+filename+"-snapshot.csv",
//                "/home/pandey/work/CALockGremlin/graphs/"+filename+"-workload.csv",
//                10);

        extractWorkloadFromJSON("/home/pandey/work/CALockGremlin/graphs/"+filename+".json",
                "/home/pandey/work/CALockGremlin/graphs/"+filename+"-snapshot.json",
                "/home/pandey/work/CALockGremlin/graphs/"+filename+"-workload.json",
                10);

        graph.loadGraphCSV("/home/pandey/work/CALockGremlin/graphs/"+filename+"-snapshot.csv");
        System.out.println("Loaded graph. Initiating labelling...");
        System.out.println("Number of vertices: "+graph.traversal().V().count().next());
        System.out.println("Number of roots: "+graph.roots.size());
        System.out.println("Number of sinks: "+graph.sinks.size());
        graph.labelGraph();
        System.out.println("Labelling complete. Initiating thread pool...");
        runBenchmark();
    }


    public static void runBenchmark(){
        Operation operation = new Operation();
        operation.computeQueryMix();

        ExecutorService threadPool= newFixedThreadPool(1);
        for(int i=0;i<1;i++){
            threadPool.submit(new workerThread(graph));
        }
        threadPool.shutdown();
    }

    public static void exrtractWorkloadFromCsv(String inputFile, String snapshot, String workload, int frequency) throws IOException, CsvValidationException {
        assert frequency > 0;
        assert frequency < 100;
        if (Files.exists(Paths.get(snapshot))) {
            Files.delete(Paths.get(snapshot));
        }
        if (Files.exists(Paths.get(workload))) {
            Files.delete(Paths.get(workload));
        }
        CSVReader reader = new CSVReader(new FileReader(inputFile));
        CSVWriter snapshotWriter = new CSVWriter(new FileWriter(snapshot));
        CSVWriter workloadWriter = new CSVWriter(new FileWriter(workload));

        String[] nextLine;
        long rowNumber = 0;

        List<String[]> snapshotRows = new ArrayList<>();
        List<String[]> workloadRows = new ArrayList<>();

        while ((nextLine = reader.readNext()) != null) {
            rowNumber++;
            if (rowNumber % frequency != 0) { // Extract every nth row to be used in the workload.
                snapshotRows.add(nextLine);
            } else {
                workloadRows.add(nextLine);
            }
        }

        snapshotWriter.writeAll(snapshotRows);
        workloadWriter.writeAll(workloadRows);

        snapshotWriter.close();
        workloadWriter.close();
        reader.close();
    }

    public static void extractWorkloadFromJSON(String inputFile, String snapshot, String workload, int frequency) throws IOException {
        assert frequency > 0;
        assert frequency < 100;
        if (Files.exists(Paths.get(snapshot))) {
            Files.delete(Paths.get(snapshot));
        }
        if (Files.exists(Paths.get(workload))) {
            Files.delete(Paths.get(workload));
        }

        FileWriter snapshotWriter = new FileWriter(snapshot);
        FileWriter workloadWriter = new FileWriter(workload);

        Set<Integer> workloadVertices = new HashSet<>();
        JsonFactory jfactory = new JsonFactory();
        JsonParser jParser = jfactory.createParser(new File(inputFile));
        Stack<JsonToken> TokenStack = new Stack<>();
        boolean vertexMode = false;
        boolean edgeMode = false;
        int objectNumber = 0;
        snapshotWriter.write("{\"mode\":\"NORMAL\"");
        workloadWriter.write("{\"mode\":\"NORMAL\"");
        JSONObject jsonObject = new JSONObject();
        do {
            JsonToken token = jParser.nextToken();
            if(token == JsonToken.START_OBJECT && ! vertexMode && ! edgeMode) {
                TokenStack.push(token);
            } else if (token == JsonToken.START_ARRAY) {
                String fieldname = jParser.getCurrentName();
                if("vertices".equals(fieldname)){
                    snapshotWriter.write(",\"vertices\":[");
                    workloadWriter.write(",\"vertices\":[");
                    vertexMode = true;
                    edgeMode = false;
                }
                if("edges".equals(fieldname)){
                    snapshotWriter.write(",\"edges\":[");
                    workloadWriter.write(",\"edges\":[");
                    vertexMode = false;
                    edgeMode = true;
                }
                TokenStack.push(token);
            } else if (token == JsonToken.START_OBJECT && vertexMode) {
                jsonObject.clear();
                while(jParser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = jParser.getCurrentName();
                    jParser.nextToken();
                    String value = jParser.getText();
                    jsonObject.put(fieldName, value);
                }
                if (objectNumber % frequency != 0) {
                    objectNumber++;
                    snapshotWriter.write(jsonObject.toString());
                    snapshotWriter.write(",");
                } else {
                    objectNumber++;
                    workloadVertices.add(jsonObject.getInt("_id"));
                    workloadWriter.write(jsonObject.toString());
                    workloadWriter.write(",");
                }
                jsonObject.clear();
            }
            else if (token == JsonToken.START_OBJECT && edgeMode) {
                jsonObject.clear();
                while(jParser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = jParser.getCurrentName();
                    jParser.nextToken();
                    String value = jParser.getText();
                    jsonObject.put(fieldName, value);
                }
                if (workloadVertices.contains(jsonObject.getInt("_inV")) || workloadVertices.contains(jsonObject.getInt("_outV"))) {
                    workloadWriter.write(jsonObject.toString());
                    workloadWriter.write(",");
                }else {
                    snapshotWriter.write(jsonObject.toString());
                    snapshotWriter.write(",");
                }
                jsonObject.clear();
            }
            else if (token == JsonToken.END_OBJECT) {
                TokenStack.pop();
            }
            else if (token == JsonToken.END_ARRAY) {
                snapshotWriter.write("{}");
                workloadWriter.write("{}");
                snapshotWriter.write("]");
                workloadWriter.write("]");
                TokenStack.pop();
            }
        } while (!TokenStack.isEmpty());
        snapshotWriter.write("}");
        workloadWriter.write("}");
        jParser.close();
        snapshotWriter.close();
        workloadWriter.close();


    }


}