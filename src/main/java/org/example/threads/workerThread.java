package org.example.threads;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.example.operations.Operation;
import org.example.operations.Queries;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.example.Main.*;

public class workerThread extends Queries implements Callable<Pair<Map<String, Integer>, Map<String, Integer>>> {
    int ThreadId;

    public workerThread(int ThreadId) {
        this.ThreadId = ThreadId;
    }

    @Override
    public Pair<Map<String, Integer>, Map<String, Integer>> call() {
        Map<String, Integer> SuccessQueryCount = new HashMap<>();
        Map<String, Integer> FailedQueryCount = new HashMap<>();
        while (!Thread.currentThread().isInterrupted() && active) {
            Pair<String, Integer> result = runQuery();
            if (result.getRight() == 1) {
                SuccessQueryCount.merge(result.getLeft(), 1, Integer::sum);
            } else {
                FailedQueryCount.merge(result.getLeft(), 1, Integer::sum);
            }
        }
        return Pair.of(SuccessQueryCount, FailedQueryCount);
    }

    public Pair<String, Integer> runQuery() {
        int range = Operation.queryDistribution.size();
        double randomNumber = generator.nextDouble();
        Pair<String, Consumer<Pair<Integer, Object[]>>> query;
        for (int i = 0; i < Operation.queryDistribution.size(); i++) {
            if (randomNumber < Operation.queryDistribution.get(i)) {
                query = Operation.queries.get(Operation.queryDistribution.get(i - 1));
                String queryName = query.getLeft();
                try {
                    switch (queryName) {
                        case "addVertex":
                            int vertex = vertexCount.addAndGet(-1);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{T.label, "newVertex", T.id, vertex}));
                            addedVertices.add(vertex);
                            break;
                        case "addEdge":
                            int edgeNumber = generator.nextInt(edgeWorkload.size());
                            Pair<Integer, Integer> edge = edgeWorkload.remove(edgeNumber);
                            removeEdgeWorkload.add(edge);
                            TinkerVertex v1 = (TinkerVertex) graph.vertex(edge.getLeft());
                            TinkerVertex v2 = (TinkerVertex) graph.vertex(edge.getRight());
                            if (v1 == null) {
                                v1 = (TinkerVertex) graph.addVertex(T.id, edge.getLeft());
                            }
                            if (v2 == null) {
                                v2 = (TinkerVertex) graph.addVertex(T.id, edge.getRight());
                            }
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{v1, v2, "newEdge", new Object[]{T.id, edge.getLeft() + "-" + edge.getRight()}}));


                            break;
                        case "setVertexProperty":
                            int vertexProp = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertexProp), "name", "newName"}));
                            break;
                        case "setEdgeProperty":
                            Pair<Integer, Integer> edgeProp = edges.get(generator.nextInt(edges.size()));
                            TinkerEdge e = (TinkerEdge) graph.edges(edgeProp.getLeft() + "-" + edgeProp.getRight()).next();
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{e, "name", "newName"}));
                            break;
                        case "removeVertex":
                            if (addedVertices.isEmpty()) {
                                break;
                            }
                            int removalVertex = addedVertices.get(generator.nextInt(addedVertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(removalVertex)}));
                            addedVertices.remove((Integer) removalVertex);
                            break;
                        case "removeEdge":
                            if (removeEdgeWorkload.isEmpty()) {
                                break;
                            }
                            int removeEdgeNumber = generator.nextInt(removeEdgeWorkload.size());
                            Pair<Integer, Integer> removeEdge = removeEdgeWorkload.remove(removeEdgeNumber);
                            edgeWorkload.add(removeEdge);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.edges(removeEdge.getLeft() + "-" + removeEdge.getRight()).next()}));
                            break;
                        case "removeVertexProperty":
                            int vertexPropRemove = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertexPropRemove), "prop"}));
                            break;
                        case "removeEdgeProperty":
                            Pair<Integer, Integer> edgePropRemove = edges.get(generator.nextInt(edges.size()));
                            TinkerEdge removeEdgeProp = (TinkerEdge) graph.edges(edgePropRemove.getLeft() + "-" + edgePropRemove.getRight()).next();
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{removeEdgeProp, "newName"}));
                            break;
                        case "getVerticesByProperty", "getEdgesByProperty":
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{"name", "newName"}));
                            break;
                        case "getEdgesByLabel":
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{"newEdge"}));
                            break;
                        case "getVertexById":
                            if (!addedVertices.isEmpty()) {
                                int vertexId = addedVertices.get(generator.nextInt(addedVertices.size()));
                                query.getRight().accept(Pair.of(ThreadId, new Object[]{vertexId}));
                            }
                            break;
                        case "getEdgeById":
                            Pair<Integer, Integer> edgeId = edges.get(generator.nextInt(edges.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{edgeId.getLeft() + "-" + edgeId.getRight()}));
                            break;
                        case "getVerticesMinKIN":
                            int degree = generator.nextInt(20);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{degree}));
                            break;
                        case "getVerticesMinKOUT":
                            int degreeOut = generator.nextInt(20);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{degreeOut}));
                            break;
                        case "getVerticesMinK":
                            int degreeBoth = generator.nextInt(20);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{degreeBoth}));
                            break;
                        case "getVerticesWithnIncomingEdges":
                            int inDegree = generator.nextInt(20);
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{inDegree}));
                            break;
                        case "BFSFromVertex":
                            int vertexBFS = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertexBFS)}));
                            break;

                        case "BFSFromVertexWithLabel":
                            int vertexBFSLabel = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertexBFSLabel), "edge"}));
                            break;
                        case "getShortestPath":
                            int vertex1 = vertices.get(generator.nextInt(vertices.size()));
                            int vertex2 = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertex1), graph.vertex(vertex2)}));
                            break;
                        case "getShortestPathWithLabel":
                            int vertex1Label = vertices.get(generator.nextInt(vertices.size()));
                            int vertex2Label = vertices.get(generator.nextInt(vertices.size()));
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{graph.vertex(vertex1Label), graph.vertex(vertex2Label), "newEdge"}));
                            break;
                        default:
                            query.getRight().accept(Pair.of(ThreadId, new Object[]{}));
                            break;
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    return Pair.of(queryName, 0);
                }
                return Pair.of(queryName, 1);
            }
        }
//        query = Operation.queries.get(Operation.queryDistribution.getLast());
        return Pair.of("none", 0);
    }
}
