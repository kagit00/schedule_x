package com.shedule.x.dto;

import com.shedule.x.dto.enums.NodeType;
import jakarta.persistence.ElementCollection;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.HashMap;
import java.util.Map;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeResponse {
    private NodeType type;
    private String referenceId;
    @ElementCollection
    private Map<String, String> metaData = new HashMap<>();
    private String groupId;

    @Override
    public String toString() {
        return "NodeResponse {\n" +
                "  type=" + type + ",\n" +
                "  referenceId='" + referenceId + "',\n" +
                "  groupId='" + groupId + "',\n" +
                "  metaData=" + metaData + "\n" +
                '}';
    }

}
