package com.shedule.x.partition;

import com.shedule.x.models.Node;
import org.apache.commons.lang3.tuple.Pair;
import java.util.List;

public interface PartitionStrategy {
    /**
     * Divides the provided list of nodes into two separate lists based on a specific partitioning strategy.
     *
     * @param nodes the list of nodes to be partitioned
     * @return a pair of lists where the first list contains nodes in the first partition
     *         and the second list contains nodes in the second partition
     */
    Pair<List<Node>, List<Node>> partition(List<Node> nodes);
}
