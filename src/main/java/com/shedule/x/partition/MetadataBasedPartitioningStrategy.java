package com.shedule.x.partition;

import com.shedule.x.models.Node;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Slf4j
public class MetadataBasedPartitioningStrategy implements PartitionStrategy {
    private final String metadataKey;
    private final String leftValue;
    private final String rightValue;


    @Override
    public Pair<List<Node>, List<Node>> partition(List<Node> nodes) {
        List<Node> left = new ArrayList<>();
        List<Node> right = new ArrayList<>();

        for (Node node : nodes) {
            Object roleValue = node.getMetaData().get(metadataKey);
            if (roleValue == null) continue;

            String role = roleValue.toString();
            if (leftValue.equalsIgnoreCase(role)) {
                left.add(node);
            } else if (rightValue.equalsIgnoreCase(role)) {
                right.add(node);
            }
        }
        return Pair.of(left, right);
    }
}
