package org.example.operations;

import locking.lockRequest;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.example.Main.*;

public class Queries {

    // Create/Update operations.
    public static void addVertex(Pair<Integer, Object[]> funcParams) {
        // Add vertex never conflicts with another operation since a new vertex is not connected and hence will never be traversed.
        // So, to include this operation in the benchmark, we will just add a random vertex with a negative ID.
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        graph.addVertex(params);
    }

    public static void addEdge(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex source = (TinkerVertex) params[0];
        TinkerVertex destination = (TinkerVertex) params[1];
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()),
                            1,
                            List.of(source.getPathLabel(), destination.getPathLabel())),
                    ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of(commonAncestors.get(commonAncestors.size() - 1)), 1, commonAncestors), ThreadId);
        }
        try {
//        System.out.println("Got lock on edge " + ThreadId);
            if (params.length <= 3)
                graph.addEdge(source, destination, (String) params[2]);
            else
                graph.addEdge(source, destination, (String) params[2], (Object[]) params[3]);
        } finally {
            lockPool.unlock(ThreadId);
        }
//        System.out.println("Released lock on edge "+ ThreadId);
    }

    public static void setVertexProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex v = (TinkerVertex) params[0];
        lockPool.lock(new lockRequest(Set.of(v.id()), 1, v.getPathLabel()), ThreadId);
//        System.out.println("Got lock on vertex " + v.id());
        try {
            v.property((String) params[1], params[2]);
        } finally {
            lockPool.unlock(ThreadId);
        }
//        System.out.println("Released lock on vertex " + v.id());
    }

    public static void setEdgeProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerEdge e = (TinkerEdge) params[0];
        TinkerVertex source = (TinkerVertex) e.outVertex();
        TinkerVertex destination = (TinkerVertex) e.inVertex();
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()), 1, List.of(source.getPathLabel(), destination.getPathLabel())), ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of((commonAncestors.get(commonAncestors.size() - 1))), 1, commonAncestors), ThreadId);
        }
        try {
            e.property((String) params[1], params[2]);
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    // Delete operations.
    public static void removeVertex(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex v = (TinkerVertex) params[0];
        lockPool.lock(new lockRequest(Set.of(v.id()), 1, v.getPathLabel()), ThreadId);
        try {
            v.remove();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void removeEdge(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerEdge e = (TinkerEdge) params[0];
        TinkerVertex source = (TinkerVertex) e.outVertex();
        TinkerVertex destination = (TinkerVertex) e.inVertex();
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()), 1, List.of(source.getPathLabel(), destination.getPathLabel())), ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of(commonAncestors.get(commonAncestors.size() - 1)), 1, commonAncestors), ThreadId);
        }
        try {
            e.remove();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void removeVertexProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex v = (TinkerVertex) params[0];
        lockPool.lock(new lockRequest(Set.of(v.id()), 1, v.getPathLabel()), ThreadId);
        try {
            v.property((String) params[1]).remove();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void removeEdgeProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerEdge e = (TinkerEdge) params[0];
        TinkerVertex source = (TinkerVertex) e.outVertex();
        TinkerVertex destination = (TinkerVertex) e.inVertex();
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()), 1, List.of(source.getPathLabel(), destination.getPathLabel())), ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of(commonAncestors.get(commonAncestors.size() - 1)), 1, commonAncestors), ThreadId);
        }
        try {
            e.property((String) params[1]).remove();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    // Simple Read operations.
    public static void getVertexCount(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        lockRequest lock = new lockRequest(Set.of(), 2, List.of());
        lockPool.lock(lock, ThreadId);
        try {
            graph.traversal().V().count().next();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getEdgeCount(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        lockRequest lock = new lockRequest(Set.of(), 2, List.of());
        lockPool.lock(lock, ThreadId);
        try {
            graph.traversal().E().count().next();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getUniqueEdgeLabels(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().E().label().dedup().toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getVerticesByProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().V().has((String) params[0], params[1]).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getEdgesByProperty(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().E().has((String) params[0], params[1]).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getEdgesByLabel(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().E().hasLabel((String) params[0]).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getVertexById(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().V(params[0]).next();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getEdgeById(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().E(params[0]).next();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }


    // Complex Reads
//
//    public static void getParents(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
////        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
//        graph.traversal().V(params[1]).in();
//    }
//
//    public static void getChildren(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
//        graph.traversal().V(params[1]).out();
//    }
//
//    public static void getNeighborsWithLabel(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
//        graph.traversal().V(params[1]).both((String) params[2]);
//    }
//
//    public static void getUniqueLabelsOfParents(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
//        graph.traversal().V(params[1]).in().label().dedup();
//    }
//
//    public static void getUniqueLabelsOfChildren(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
//        graph.traversal().V(params[1]).out().label().dedup();
//    }
//
//    public static void getUniqueLabelsOfNeighbors(Pair<Integer, Object[]> funcParams) {
//        int ThreadId = funcParams.getLeft();
//        Object[] params = funcParams.getRight();
//        graph.traversal().V(params[1]).both().label().dedup();
//    }


    // Long Traversals
    public static void getVerticesMinKIN(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().V().filter(inE().count().is(gt(params[0]))).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getVerticesMinK(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().V().filter(bothE().count().is(gt(params[0]))).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getVerticesWithnIncomingEdges(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        lockPool.lock(new lockRequest(Set.of(), 2, List.of()), ThreadId);
        try {
            graph.traversal().V().filter(inE().count().is(params[0])).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void BFSFromVertex(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex v = (TinkerVertex) params[1];
        lockPool.lock(new lockRequest(Set.of(v.id()), 2, v.getPathLabel()), ThreadId);
        try {
            graph.traversal().V(params[1]).repeat(__.out()).times(1).dedup();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void BFSFromVertexWithLabel(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex v = (TinkerVertex) params[0];
        lockPool.lock(new lockRequest(Set.of(v.id()), 2, v.getPathLabel()), ThreadId);
        try {
            graph.traversal().V(params[0]).repeat(__.out((String) params[1])).times(1).dedup().toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getShortestPath(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex source = (TinkerVertex) params[0];
        TinkerVertex destination = (TinkerVertex) params[1];
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()), 1, List.of(source.getPathLabel(), destination.getPathLabel())), ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of(commonAncestors.get(commonAncestors.size() - 1)), 1, commonAncestors), ThreadId);
        }
        try {
            graph.traversal().V(source.id()).repeat(__.out().simplePath()).until(__.hasId(destination.id())).path().limit(1).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }

    public static void getShortestPathWithLabel(Pair<Integer, Object[]> funcParams) {
        int ThreadId = funcParams.getLeft();
        Object[] params = funcParams.getRight();
        TinkerVertex source = (TinkerVertex) params[0];
        TinkerVertex destination = (TinkerVertex) params[1];
        List<Object> commonAncestors = source.getPathLabel().stream().filter(destination.getPathLabel()::contains).toList();
        if (commonAncestors.isEmpty()) {
            lockPool.lock(new lockRequest(Set.of(source.id(), destination.id()), 1, List.of(source.getPathLabel(), destination.getPathLabel())), ThreadId);
        } else {
            lockPool.lock(new lockRequest(Set.of(commonAncestors.get(commonAncestors.size() - 1)), 1, commonAncestors), ThreadId);
        }
        try {
            graph.traversal().V(source.id()).repeat(__.out((String) params[2]).simplePath()).until(__.hasId(destination.id())).path().limit(1).toList();
        } finally {
            lockPool.unlock(ThreadId);
        }
    }
}