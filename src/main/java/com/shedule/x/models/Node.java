package com.shedule.x.models;

import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.validation.ValidEnum;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "nodes")
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "nodeCache")
public class Node {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @Column(nullable = false)
    @ValidEnum(enumClass = NodeType.class, message = "Invalid node type")
    private String type;
    @Column(name = "reference_id", nullable = false)
    private String referenceId;
    @ElementCollection(fetch = FetchType.LAZY)
    @CollectionTable(name = "node_metadata", joinColumns = @JoinColumn(name = "node_id"))
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value")
    private Map<String, String> metaData = new HashMap<>();
    @Column(name = "group_id", nullable = false)
    private String groupId;
    private LocalDateTime createdAt;
    @Column(nullable = false)
    private UUID domainId;
    @Column(name = "is_processed")
    private boolean processed = false;
}
