package com.shedule.x.dto;


import lombok.Builder;

@Builder
public record NodesCount(boolean hasMoreNodes, int nodeCount) {
}
