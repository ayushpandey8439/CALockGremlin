package org.example.operations;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    static double complexReads = 0.3;
    static double simpleReads = 0.5;
    static double longTraversal = 0.1;

    static Map<Double, Consumer<Object[]>> queries = new HashMap<>();
    static List<Double> queryDistribution = new ArrayList<>();
    public void computeQueryMix() {

        try {
            Configurations configs = new Configurations();
            Configuration config = configs.properties(new File("queryMix.properties"));
            update = config.getDouble("updates");
            reads = config.getDouble("reads");
            simpleReads = config.getDouble("simpleReads");
            complexReads = config.getDouble("complexReads");
            longTraversal = reads-simpleReads-complexReads;
            if (longTraversal < 0){
                throw new ConfigurationException("No Long traversals inluded in the query mix.");
            }
            if(simpleReads + complexReads + update + longTraversal != 1.0){
                throw new ConfigurationException("The sum of the query mix should be 1.0");
            }

            int UpdateCount = 9;
            int SimpleReadCount = 8;
            int ComplexReadCount = 6;
            int LongTraversalCount = 8;

            double cdf = 0.0;
            queryDistribution.add(cdf);

            // Create/Update/Delete operations.
            queries.put(cdf, Queries::addVertex);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::addEdge);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::setVertexProperty);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::setEdgeProperty);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::removeVertex);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::removeEdge);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::removeVertexProperty);
            cdf += update/UpdateCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::removeEdgeProperty);


            // Read operations.
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVertexCount);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getEdgeCount);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getUniqueEdgeLabels);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVerticesByProperty);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getEdgesByProperty);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getEdgesByLabel);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVertexById);
            cdf += simpleReads/SimpleReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getEdgeById);


            //Complex reads
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getParents);
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getChildren);
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getNeighborsWithLabel);
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getUniqueLabelsOfParents);
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getUniqueLabelsOfChildren);
            cdf += complexReads/ComplexReadCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getUniqueLabelsOfNeighbors);

            //Long Traversal
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVerticesMinKIN);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVerticesMinKOUT);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVerticesMinK);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getVerticesWithnIncomingEdges);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::BFSFromVertex);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::BFSFromVertexWithLabel);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getShortestPath);
            cdf += longTraversal/LongTraversalCount;
            queryDistribution.add(cdf);
            queries.put(cdf, Queries::getShortestPathWithLabel);

            System.out.println("Query mix computed" + queryDistribution.size());

        } catch (ConfigurationException cex){
            System.out.println(cex.getMessage());
        }
    }
}
