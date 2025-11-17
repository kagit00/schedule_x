package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "nodes_cursor")
public class NodesCursor {

    @EmbeddedId
    private NodesCursorId id;

    @Column(name = "cursor_created_at")
    private OffsetDateTime cursorCreatedAt;

    @Column(name = "cursor_id")
    private UUID cursorId;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;
}

