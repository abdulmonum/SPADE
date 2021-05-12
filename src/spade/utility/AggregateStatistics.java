package spade.utility;

import spade.core.AbstractEdge;
import spade.core.AbstractVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import spade.core.Graph;

public class AggregateStatistics
{
    public enum ElementType
    {
        VERTEX,
        EDGE
    }
    public enum AggregateType
    {
        HISTOGRAM,
        MEAN,
        STD
    }

    private static Map<String, Integer> computeVertexHistogram(String key, Set<AbstractVertex> vertexSet)
    {
        Map<String, Integer> histogram = new HashMap<>();
        for(AbstractVertex vertex: vertexSet)
        {
            String value = vertex.getAnnotation(key);
            histogram.merge(value, 1, Integer::sum);
        }
        return histogram;
    }

    private static Map<String, Integer> computeEdgeHistogram(String key, Set<AbstractEdge> edgeSet)
    {
        Map<String, Integer> histogram = new HashMap<>();
        for(AbstractEdge edge: edgeSet)
        {
            String value = edge.getAnnotation(key);
            histogram.merge(value, 1, Integer::sum);
        }
        return histogram;
    }

    public static Map<String, Integer> computeHistogram(Graph graph, String key,
                                                        ElementType elementType)
    {
        Map<String, Integer> histogram = null;
        if(elementType.equals(ElementType.VERTEX))
        {
            histogram = computeVertexHistogram(key, graph.vertexSet());
        }
        else if(elementType.equals(ElementType.EDGE))
        {
            histogram = computeEdgeHistogram(key, graph.edgeSet());
        }
        return histogram;
    }
}

