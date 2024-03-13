package org.example;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import locking.lockPool;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileBased;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.example.operations.Operation;
import org.example.threads.workerThread;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newFixedThreadPool;

public class Main {
    public static TinkerGraph graph = TinkerGraph.open();
    public static ImmutableConfiguration config;
    static Parameters params = new Parameters();
    public static AtomicInteger vertexCount = new AtomicInteger(-1);
    public static boolean active = true;
    public static List<Integer> vertices = new ArrayList<>();
    public static List<Pair<Integer, Integer>> edges = new ArrayList<>();

    public static List<Integer> addedVertices = new ArrayList<>();
    public static List<Pair<Integer, Integer>> edgeWorkload = new ArrayList<>();
    public static List<Pair<Integer, Integer>> removeEdgeWorkload = new ArrayList<>();
    public static Random generator;

    static FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
            new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                    .configure(params.properties()
                            .setFileName("benchmarkParameters.properties")
                            .setListDelimiterHandler(new DefaultListDelimiterHandler(',')));

    static {
        try {
            config = builder.getConfiguration();
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public static lockPool lockPool = new lockPool();


    public static void main(String[] args) throws IOException, CsvValidationException, InterruptedException {
        graph = TinkerGraph.open();
        String filename = config.getString("benchmarkData");
        String type = config.getString("benchmarkDataType");
        generator = new Random(Long.parseLong(config.getString("randomSeed")));
        if (type.equals("json")) {
            extractWorkloadFromJSON(config.getString("dataPath") + filename + ".json",
                    config.getString("dataPath") + filename + "-snapshot.json",
                    config.getString("dataPath") + filename + "-workload.json",
                    10);
            graph.loadGraphSON(config.getString("dataPath") + filename + "-snapshot.json");
        } else {
            exrtractWorkloadFromCsv(config.getString("dataPath") + filename + ".csv",
                    config.getString("dataPath") + filename + "-snapshot.csv",
                    config.getString("dataPath") + filename + "-workload.csv",
                    10);
            graph.loadGraphCSV(config.getString("dataPath") + filename + "-snapshot.csv");
        }
        System.out.println("Threads: " + config.getInt("threads") + " Duration: " + config.getLong("duration")/1000 + "s");
        System.out.println("Lock Type: " + config.getString("lockType"));
        System.out.println("Loaded graph. Initiating labelling...");
        System.out.println("Number of vertices: " + graph.traversal().V().count().next());
        System.out.println("Number of roots: " + graph.roots.size());
        System.out.println("Number of sinks: " + graph.sinks.size());
        graph.labelGraph();
        System.out.println("Labelling complete. Starting benchmark...");
        runBenchmark();
    }


    public static void runBenchmark() throws InterruptedException {
        Operation operation = new Operation();
        operation.computeQueryMix();
        Set<Future<Pair<Map<String, Integer>, Map<String,Integer>>>> queryResults = new HashSet<>();
        ExecutorService threadPool = newFixedThreadPool(config.getInt("threads"));


        for (int i = 0; i < config.getInt("threads"); i++) {
            Future<Pair<Map<String, Integer>, Map<String,Integer>>> result = threadPool.submit(new workerThread(i));
            queryResults.add(result);
        }
        Thread.sleep(config.getLong("duration"));
        active = false;
        System.out.println("Shutting down benchmark run.");
        try{
            threadPool.shutdown();
            if(!threadPool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)){
                System.err.println("Waiting for thread pool to finish.");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e){
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            showResults(queryResults);
        }

    }

    public static void showResults(Set<Future<Pair<Map<String, Integer>, Map<String,Integer>>>> queryResults) {
        Map<String, Integer> SuccessCombinedQueryFrequency = new HashMap<>();
        Map<String, Integer> FailedCombinedQueryFrequency = new HashMap<>();

        for (Future<Pair<Map<String, Integer>, Map<String,Integer>>>result : queryResults) {
            try {
                Map<String, Integer> frequency = result.get().getLeft();
                for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
                    SuccessCombinedQueryFrequency.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
                frequency = result.get().getRight();
                for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
                    FailedCombinedQueryFrequency.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        SuccessCombinedQueryFrequency.remove("none");
        FailedCombinedQueryFrequency.remove("none");

        System.out.println("Success Query");
        int count = 0;
        for (Map.Entry<String, Integer> entry : SuccessCombinedQueryFrequency.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
            count += entry.getValue();
        }
        System.out.println("Failed Query");
        for (Map.Entry<String, Integer> entry : FailedCombinedQueryFrequency.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
//            count += entry.getValue();
        }
        System.out.println("Total Queries: " + count +" in "+ config.getLong("duration") + " ms");
        System.out.println("Latency: " + lockPool.latency + " ms");
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

        for (String[] row : workloadRows) {
            edgeWorkload.add(Pair.of(Integer.parseInt(row[0]), Integer.parseInt(row[1])));
        }
        for (String[] row : snapshotRows) {
            vertices.add(Integer.parseInt(row[0]));
            vertices.add(Integer.parseInt(row[1]));
            edges.add(Pair.of(Integer.parseInt(row[0]), Integer.parseInt(row[1])));
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
        Stack<JsonToken> TokenStack = new Stack<JsonToken>();
        boolean vertexMode = false;
        boolean edgeMode = false;
        int objectNumber = 0;
        snapshotWriter.write("{\"mode\":\"NORMAL\"");
        workloadWriter.write("{\"mode\":\"NORMAL\"");
        JSONObject jsonObject = new JSONObject();
        do {
            JsonToken token = jParser.nextToken();
            if (token == JsonToken.START_OBJECT && !vertexMode && !edgeMode) {
                TokenStack.push(token);
            } else if (token == JsonToken.START_ARRAY) {
                String fieldname = jParser.getCurrentName();
                if ("vertices".equals(fieldname)) {
                    snapshotWriter.write(",\"vertices\":[");
                    workloadWriter.write(",\"vertices\":[");
                    vertexMode = true;
                    edgeMode = false;
                }
                if ("edges".equals(fieldname)) {
                    snapshotWriter.write(",\"edges\":[");
                    workloadWriter.write(",\"edges\":[");
                    vertexMode = false;
                    edgeMode = true;
                }
                TokenStack.push(token);
            } else if (token == JsonToken.START_OBJECT && vertexMode) {
                jsonObject.clear();
                while (jParser.nextToken() != JsonToken.END_OBJECT) {
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
            } else if (token == JsonToken.START_OBJECT && edgeMode) {
                jsonObject.clear();
                while (jParser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = jParser.getCurrentName();
                    jParser.nextToken();
                    String value = jParser.getText();
                    jsonObject.put(fieldName, value);
                }
                if (workloadVertices.contains(jsonObject.getInt("_inV")) || workloadVertices.contains(jsonObject.getInt("_outV"))) {
                    workloadWriter.write(jsonObject.toString());
                    workloadWriter.write(",");
                } else {
                    snapshotWriter.write(jsonObject.toString());
                    snapshotWriter.write(",");
                }
                jsonObject.clear();
            } else if (token == JsonToken.END_OBJECT) {
                TokenStack.pop();
            } else if (token == JsonToken.END_ARRAY) {
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