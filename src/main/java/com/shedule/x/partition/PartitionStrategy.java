package com.shedule.x.partition;

import com.shedule.x.models.Node;
import org.apache.commons.lang3.tuple.Pair;
import java.util.List;
import java.util.stream.Stream;

public interface PartitionStrategy {
    Pair<Stream<Node>, Stream<Node>> partition(Stream<Node> nodes, String key, String leftValue, String rightValue);
}
