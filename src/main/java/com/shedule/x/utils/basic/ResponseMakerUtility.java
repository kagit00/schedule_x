package com.shedule.x.utils.basic;


import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.models.PotentialMatchEntity;
import java.util.Map;


public final class ResponseMakerUtility {

    private ResponseMakerUtility() {
        throw new UnsupportedOperationException("Unsupported operation");
    }


    public static NodeResponse generateNodeResponse(NodeType type, String referenceId, Map<String, String> metaData, String groupId) {
        return NodeResponse.builder()
                .type(type)
                .referenceId(referenceId)
                .metaData(metaData)
                .groupId(groupId)
                .build();
    }

    public static MatchTransfer buildMatchTransfer(PerfectMatchEntity p) {
        return MatchTransfer.builder()
                .groupId(p.getGroupId())
                .referenceId(p.getReferenceId())
                .matchedReferenceId(p.getMatchedReferenceId())
                .compatibilityScore(p.getCompatibilityScore())
                .build();
    }

    public static MatchTransfer buildMatchTransfer(PotentialMatchEntity p) {
        return MatchTransfer.builder()
                .groupId(p.getGroupId())
                .referenceId(p.getReferenceId())
                .matchedReferenceId(p.getMatchedReferenceId())
                .compatibilityScore(p.getCompatibilityScore())
                .build();
    }
}
