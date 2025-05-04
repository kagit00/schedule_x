package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "edges")
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Edge {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "from_node", nullable = false)
    private Node fromNode;
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "to_node", nullable = false)
    private Node toNode;
    private double weight;
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "edge_metadata", joinColumns = @JoinColumn(name = "edge_id"))
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value")
    private Map<String, String> metaData = new HashMap<>();
}
