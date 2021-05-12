package spade.transformer;

import spade.client.QueryMetaData;
import spade.core.AbstractTransformer;
import spade.core.Graph;
import com.google.privacy.differentialprivacy.Count;

import spade.utility.AggregateStatistics.AggregateType;
import spade.utility.AggregateStatistics.ElementType;

import java.util.*;

public class DifferentialPrivacy extends AbstractTransformer
{
    private static final double EPSILON = Math.log(3);

    /** algorithm for getting private histogram based on the
     * description and use case as described in:
     * https://github.com/google/differential-privacy/blob/main/examples/java/CountVisitsPerHour.java
    */

    @Override
    public Graph transform(Graph graph, QueryMetaData queryMetaData)
    {
//        Map<String, Integer> histogram = (Map<String, Integer>) graph.getResultMetaData()
//                                        .getMetaData(queryMetaData.getAnnotationName(),
//                                        AggregateType.HISTOGRAM,
//                                        ElementType.VERTEX);

//        List<Integer> list = (List<Integer>) histogram.values();
        List<Integer> list = Arrays.asList(14, 6, 3, 21, 4);
        SortedMap<Integer, Count> dpCounts = new TreeMap<>();
        System.out.println(list);
        for (int i = 0; i < list.size(); i++)
        {
            Count dpCount = Count
                    .builder()
                    .epsilon(EPSILON)
                    .maxPartitionsContributed(1)
                    .build();
            dpCount.incrementBy(list.get(i));
            dpCounts.put(i, dpCount);
        }
        List<Integer> privateList = new ArrayList<>();
        for(Map.Entry<Integer, Count> dpCount: dpCounts.entrySet())
        {
            privateList.add((int) dpCount.getValue().computeResult());
        }
        graph.getResultMetaData().setMetaData(queryMetaData.getAnnotationName(),
                                        AggregateType.HISTOGRAM,
                                        ElementType.VERTEX,
                                        privateList);
        System.out.println(privateList);

        return null;
    }
}
