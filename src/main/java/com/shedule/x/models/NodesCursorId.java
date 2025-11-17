package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;
import java.util.UUID;

@Embeddable
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NodesCursorId implements Serializable {

    @Column(name = "group_id")
    private UUID groupId;

    @Column(name = "domain_id")
    private UUID domainId;
}
