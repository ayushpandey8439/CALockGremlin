package org.example.operations;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Consumer;

import static org.example.Main.config;

public class Operation {

    // In a social network, this means the workload is read-dominated: for instance, Facebook recently
    // revealed that for each 500 reads there is 1 write in their social network [15].

    // When calibrating SNB-Interactive query mix we aimed at 10% of total runtime to be taken by update
    // queries (taken from the data generator), 50% of time take complex readonly queries, and 40% for the
    // simple read-only queries. Within the corresponding shares of time, we make sure each query type
    // takes approximately equal amount of CPU time (i.e., queries that touch more data run less frequently)
    // to avoid the workload being dominated by a single query.

    // TODO: A Query that takes longer to run should run infrequently. But should be eligible to run at all times.
    //  For example. BFSFromVertexWithLabel should run less frequently than getVerticesByProperty. But the runtime
    //  workload should have about 50% of the total queries from the complex traversal category.

    static double update = 0.1;
    static double reads = 0.9;
    static double simpleReads = 0.5;
    static double longTraversal = 0.1;

    public static Map<Double, Pair<String, Consumer<Pair<Integer, Object[]>>>> queries = new HashMap<>();
    public static LinkedList<Double> queryDistribution = new LinkedList<>();
    public static Map<String, Consumer<Pair<Integer, Object[]>>> queryList = new HashMap<>();

    public void computeQueryMix() {

        try {
            update = config.getDouble("updates");
            reads = 1.0-update;
            simpleReads = reads*config.getDouble("simpleReads");
//            complexReads = reads*config.getDouble("complexReads");
            longTraversal = reads - simpleReads;
            if (longTraversal < 0) {
                throw new ConfigurationException("No Long traversals inluded in the query mix.");
            }
            if (simpleReads + update + longTraversal != 1.0) {
                throw new ConfigurationException("The sum of the query mix should be 1.0");
            }

//            int UpdateCount = 9;
//            int SimpleReadCount = 8;
//            int ComplexReadCount = 6;
//            int LongTraversalCount = 8;

            double cdf = 0.0;
//            queryDistribution.add(cdf);

            List<Object> CRUDQueries = config.getList("CRUDQueries");

            for (Object query : CRUDQueries) {
                String queryName = (String) query;
                if(queryName.isEmpty()){continue;}
                queries.put(cdf, Pair.of(queryName, queryList.get(queryName)));
                queryDistribution.add(cdf);
                cdf += update / CRUDQueries.size();

            }

            List<Object> SimpleReadQueries = config.getList("SimpleReadQueries");
            for (Object query : SimpleReadQueries) {
                String queryName = (String) query;
                if(queryName.isEmpty()){continue;}
                queries.put(cdf, Pair.of(queryName, queryList.get(queryName)));
                queryDistribution.add(cdf);
                cdf += simpleReads / SimpleReadQueries.size();
            }
//
//            List<Object> ComplexReadQueries = config.getList("ComplexReadQueries");
//            for (Object query : ComplexReadQueries) {
//                String queryName = (String) query;
//                if(queryName.isEmpty()){continue;}
//                queries.put(cdf, Pair.of(queryName, queryList.get(queryName)));
//                queryDistribution.add(cdf);
//                cdf += complexReads / ComplexReadQueries.size();
//            }

            List<Object> LongTraversalQueries = config.getList("LongTraversalQueries");
            for (Object query : LongTraversalQueries) {
                String queryName = (String) query;
                if(queryName.isEmpty()){continue;}
                queries.put(cdf, Pair.of(queryName, queryList.get(queryName)));
                queryDistribution.add(cdf);
                cdf += longTraversal / LongTraversalQueries.size();

            }
            queryDistribution.add(1.0);

            System.out.println("Query mix computed");

        } catch (ConfigurationException cex) {
            System.out.println(cex.getMessage());
        }


    }

    public Operation() {
        queryList.put("addVertex", Queries::addVertex);
        queryList.put("addEdge", Queries::addEdge);
        queryList.put("setVertexProperty", Queries::setVertexProperty);
        queryList.put("setEdgeProperty", Queries::setEdgeProperty);
        queryList.put("removeVertex", Queries::removeVertex);
        queryList.put("removeEdge", Queries::removeEdge);
        queryList.put("removeVertexProperty", Queries::removeVertexProperty);
        queryList.put("removeEdgeProperty", Queries::removeEdgeProperty);
        queryList.put("getVertexCount", Queries::getVertexCount);
        queryList.put("getEdgeCount", Queries::getEdgeCount);
        queryList.put("getUniqueEdgeLabels", Queries::getUniqueEdgeLabels);
        queryList.put("getVerticesByProperty", Queries::getVerticesByProperty);
        queryList.put("getEdgesByProperty", Queries::getEdgesByProperty);
        queryList.put("getEdgesByLabel", Queries::getEdgesByLabel);
        queryList.put("getVertexById", Queries::getVertexById);
        queryList.put("getEdgeById", Queries::getEdgeById);
//        queryList.put("getParents", Queries::getParents);
//        queryList.put("getChildren", Queries::getChildren);
//        queryList.put("getNeighborsWithLabel", Queries::getNeighborsWithLabel);
//        queryList.put("getUniqueLabelsOfParents", Queries::getUniqueLabelsOfParents);
//        queryList.put("getUniqueLabelsOfChildren", Queries::getUniqueLabelsOfChildren);
//        queryList.put("getUniqueLabelsOfNeighbors", Queries::getUniqueLabelsOfNeighbors);
        queryList.put("getVerticesMinKIN", Queries::getVerticesMinKIN);
        queryList.put("getVerticesMinK", Queries::getVerticesMinK);
        queryList.put("getVerticesWithnIncomingEdges", Queries::getVerticesWithnIncomingEdges);
        queryList.put("BFSFromVertex", Queries::BFSFromVertex);
        queryList.put("BFSFromVertexWithLabel", Queries::BFSFromVertexWithLabel);
        queryList.put("getShortestPath", Queries::getShortestPath);
        queryList.put("getShortestPathWithLabel", Queries::getShortestPathWithLabel);

    }
}
