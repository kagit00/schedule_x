package com.shedule.x.partition;

import com.shedule.x.models.Node;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component("metadataBasedPartitioningStrategy")
public class MetadataBasedPartitioningStrategy implements PartitionStrategy {

    @Override
    public Pair<Stream<Node>, Stream<Node>> partition(Stream<Node> nodes, String key, String leftValue, String rightValue) {
        List<Node> nodeList = nodes.collect(Collectors.toList());

        List<Node> left = nodeList.stream()
                .filter(node -> leftValue.equals(node.getMetaData().getOrDefault(key, "")))
                .collect(Collectors.toList());

        List<Node> right = nodeList.stream()
                .filter(node -> rightValue.equals(node.getMetaData().getOrDefault(key, "")))
                .collect(Collectors.toList());

        return Pair.of(left.stream(), right.stream());
    }

}