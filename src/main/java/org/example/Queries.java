package org.example;

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

public abstract class Queries {

    // Create operations.
    public Vertex addVertex(TinkerGraph graph, Object id, Object... props){
        return graph.addVertex(id, props);
    }
    public Edge addEdge(TinkerGraph graph, TinkerVertex source, TinkerVertex target, String label){
        return graph.addEdge(source, target, label);
    }
    public Edge addEdge(TinkerGraph graph, TinkerVertex source, TinkerVertex target, String label, Object... props){
        return graph.addEdge(source, target, label, props);
    }
    public Property<Object> setProperty(TinkerVertex vertex, String key, Object value){
        return vertex.property(key, value);
    }
    public Property<Object> setProperty(TinkerEdge edge, String key, Object value){
        return edge.property(key, value);
    }
    // Read operations.
    public Number getVertexCount(TinkerGraph graph){
        return graph.traversal().V().count().next();
    }
    public Number getEdgeCount(TinkerGraph graph){
        return graph.traversal().E().count().next();
    }
    public List<String> getUniqueEdgeLabels(TinkerGraph graph){
        return graph.traversal().E().label().dedup().toList();
    }
    public List<Vertex> getVerticesByProperty(TinkerGraph graph, String key, Object value){
        return graph.traversal().V().has(key, value).toList();
    }
    public List<Edge> getEdgesByProperty(TinkerGraph graph, String key, Object value){
        return graph.traversal().E().has(key, value).toList();
    }
    public List<Edge> getEdgesByLabel(TinkerGraph graph, String label){
        return graph.traversal().E().hasLabel(label).toList();
    }
    public Vertex getVertexById(TinkerGraph graph, Object id){
        return graph.traversal().V(id).next();
    }
    public Edge getEdgeById(TinkerGraph graph, Object id){
        return graph.traversal().E(id).next();
    }
    // Delete operations.
    public void removeVertex(TinkerGraph graph, TinkerVertex vertex){
        graph.removeVertex(vertex);
    }
    public void removeEdge(TinkerGraph graph, TinkerEdge edge){
        graph.removeEdge(edge);
    }
    public void removeVertexProperty(TinkerVertex vertex, String key){
        vertex.property(key).remove();
    }
    public void removeEdgeProperty(TinkerEdge edge, String key){
        edge.property(key).remove();
    }

    // Traversals.
    public List<Vertex> getParents(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).in().toList();
    }
    public List<Vertex> getChildren(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).out().toList();
    }
    public List<Vertex> getNeighborsWithLabel(TinkerGraph graph, TinkerVertex vertex, String label){
        return graph.traversal().V(vertex).both(label).toList();
    }
    public List<String> getUniqueLabelsOfParents(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).in().label().dedup().toList();
    }
    public List<String> getUniqueLabelsOfChildren(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).out().label().dedup().toList();
    }
    public List<String> getUniqueLabelsOfNeighbors(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).both().label().dedup().toList();
    }
    public List<Vertex> getVerticesMinKIN(TinkerGraph graph, TinkerVertex vertex, int k){
        return graph.traversal().V().filter(__.inE().count().is(k)).toList();
    }
    public List<Vertex> getVerticesMinKOUT(TinkerGraph graph, TinkerVertex vertex, int k){
        return graph.traversal().V().filter(__.outE().count().is(k)).toList();
    }
    public List<Vertex> getVerticesMinK(TinkerGraph graph, TinkerVertex vertex, int k){
        return graph.traversal().V().filter(__.bothE().count().is(k)).toList();
    }
    public List<Vertex> getVerticesWitnIncomingEdges(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V().outE().V().dedup().toList();
    }
    public List<Vertex> BFSFromVertex(TinkerGraph graph, TinkerVertex vertex){
        return graph.traversal().V(vertex).repeat(__.out()).times(2).dedup().toList();
    }
    public List<Vertex> BFSFromVertexWithLabel(TinkerGraph graph, TinkerVertex vertex, String label){
        return graph.traversal().V(vertex).repeat(__.out(label)).times(2).dedup().toList();
    }
    public List<Path> getShortestPath(TinkerGraph graph, TinkerVertex source, TinkerVertex target){
        return graph.traversal().V(source).repeat(__.out().simplePath()).until(__.is(P.within(target))).path().limit(1).toList();
    }
    public List<Path> getShortestPathWithLabel(TinkerGraph graph, TinkerVertex source, TinkerVertex target, String label){
        return graph.traversal().V(source).repeat(__.out(label).simplePath()).until(__.is(P.within(target))).path().limit(1).toList();
    }






}
