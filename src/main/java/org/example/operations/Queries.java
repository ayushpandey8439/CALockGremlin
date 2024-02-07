package org.example.operations;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.util.List;

public class Queries {

    // Create/Update operations.
    public static void addVertex(Object... params){
        ((TinkerGraph) params[0]).addVertex(params[1]);
    }
    public static void addEdge(Object... params){
         ((TinkerGraph) params[0]).addEdge((TinkerVertex) params[1], (TinkerVertex) params[2], (String) params[3], params[4]);
    }

    public static void setVertexProperty(Object... params){
        ((TinkerVertex) params[0]).property((String) params[1], params[2]);
    }
    public static void setEdgeProperty(Object... params){
        ((TinkerEdge) params[0]).property((String) params[1], params[2]);
    }

    // Delete operations.
    public static void removeVertex(Object... params){
        ((TinkerGraph) params[0]).removeVertex((TinkerVertex) params[1]);
    }
    public static void removeEdge(Object... params){
        ((TinkerGraph) params[0]).removeEdge((TinkerEdge) params[1]);
    }
    public static void removeVertexProperty(Object... params){
        ((TinkerVertex) params[0]).property((String) params[1]).remove();
    }
    public static void removeEdgeProperty(Object... params){
        ((TinkerEdge) params[0]).property((String) params[1]).remove();
    }

    // Simple Read operations.
    public static void getVertexCount(Object... params){
        ((TinkerGraph) params[0]).traversal().V().count().next();
    }
    public static void getEdgeCount(Object... params){
        ((TinkerGraph) params[0]).traversal().E().count().next();
    }
    public static void getUniqueEdgeLabels(Object... params){
        ((TinkerGraph) params[0]).traversal().E().label().dedup().toList();
    }
    public static void getVerticesByProperty(Object... params){
        ((TinkerGraph) params[0]).traversal().V().has((String) params[1], params[2]).toList();
    }
    public static void getEdgesByProperty(Object... params){
        ((TinkerGraph) params[0]).traversal().E().has((String) params[1], params[2]).toList();
    }
    public static void getEdgesByLabel(Object... params){
        ((TinkerGraph) params[0]).traversal().E().hasLabel((String) params[1]).toList();
    }
    public static void getVertexById(Object... params){
        ((TinkerGraph) params[0]).traversal().V(params[1]).next();
    }
    public static void getEdgeById(Object... params){
        ((TinkerGraph) params[0]).traversal().E(params[1]).next();
    }


    // Complex Reads

    public static void getParents(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).in().toList();
    }
    public static void getChildren(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).out().toList();
    }
    public static void getNeighborsWithLabel(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).both((String) params[2]).toList();
    }
    public static void getUniqueLabelsOfParents(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).in().label().dedup().toList();
    }
    public static void getUniqueLabelsOfChildren(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).out().label().dedup().toList();
    }
    public static void getUniqueLabelsOfNeighbors(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).both().label().dedup().toList();
    }


    // Long Traversals
    public static void getVerticesMinKIN(Object... params){
        ((TinkerGraph) params[0]).traversal().V().filter(__.inE().count().is(params[1])).toList();
    }
    public static void getVerticesMinKOUT(Object... params){
        ((TinkerGraph) params[0]).traversal().V().filter(__.outE().count().is(params[1])).toList();
    }
    public static void getVerticesMinK(Object... params){
        ((TinkerGraph) params[0]).traversal().V().filter(__.bothE().count().is(params[1])).toList();
    }
    public static void getVerticesWithnIncomingEdges(Object... params){
        ((TinkerGraph) params[0]).traversal().V().outE().V().dedup().toList();
    }
    public static void BFSFromVertex(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).repeat(__.out()).times(2).dedup().toList();
    }
    public static void BFSFromVertexWithLabel(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).repeat(__.out((String) params[2])).times(2).dedup().toList();
    }
    public static void getShortestPath(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).repeat(__.out().simplePath()).until(__.is(P.within((TinkerVertex) params[2]))).path().limit(1).toList();
    }
    public static void getShortestPathWithLabel(Object... params){
        ((TinkerGraph) params[0]).traversal().V((TinkerVertex) params[1]).repeat(__.out((String) params[3]).simplePath()).until(__.is(P.within((TinkerVertex) params[2]))).path().limit(1).toList();
    }
}