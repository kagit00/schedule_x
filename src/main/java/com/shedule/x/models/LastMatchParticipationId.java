package com.shedule.x.models;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Embeddable
public class LastMatchParticipationId implements Serializable {
    @Column(name = "group_id")
    private String groupId;

    @Column(name = "domain_id")
    private UUID domainId;

    public LastMatchParticipationId() {}

    public LastMatchParticipationId(String groupId, UUID domainId) {
        this.groupId = groupId;
        this.domainId = domainId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LastMatchParticipationId that = (LastMatchParticipationId) o;
        return groupId.equals(that.groupId) && domainId.equals(that.domainId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, domainId);
    }
}