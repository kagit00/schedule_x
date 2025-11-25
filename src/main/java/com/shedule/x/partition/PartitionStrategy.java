package com.shedule.x.partition;

import com.shedule.x.dto.NodeDTO;
import org.apache.commons.lang3.tuple.Pair;
import java.util.stream.Stream;

public interface PartitionStrategy {
    Pair<Stream<NodeDTO>, Stream<NodeDTO>> partition(Stream<NodeDTO> nodes, String key, String leftValue, String rightValue);
}
