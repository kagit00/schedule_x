package com.shedule.x.processors;

import com.shedule.x.config.factory.CopyStreamSerializer;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.DefaultValuesPopulator;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;


public class PotentialMatchSerializer implements CopyStreamSerializer<PotentialMatchEntity> {
    private final List<PotentialMatchEntity> batch;
    private final UUID groupId;
    private final UUID domainId;
    private final String processingCycleId;

    public PotentialMatchSerializer(List<PotentialMatchEntity> batch, UUID groupId, UUID domainId, String processingCycleId) {
        this.batch = batch;
        this.groupId = groupId;
        this.domainId = domainId;
        this.processingCycleId = processingCycleId != null ? processingCycleId : "";
    }

    @Override
    public void serialize(PotentialMatchEntity match, DataOutputStream out) throws IOException {
        out.writeShort(8); // number of columns

        // UUID id
        out.writeInt(16);
        UUID id = match.getId() != null ? match.getId() : DefaultValuesPopulator.getUid2();
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());

        // groupId
        out.writeInt(16);
        out.writeLong(groupId.getMostSignificantBits());
        out.writeLong(groupId.getLeastSignificantBits());

        // domainId
        out.writeInt(16);
        out.writeLong(domainId.getMostSignificantBits());
        out.writeLong(domainId.getLeastSignificantBits());

        // processingCycleId
        byte[] cycleIdBytes = processingCycleId.getBytes(StandardCharsets.UTF_8);
        out.writeInt(cycleIdBytes.length);
        out.write(cycleIdBytes);

        // referenceId
        byte[] refIdBytes = match.getReferenceId().getBytes(StandardCharsets.UTF_8);
        out.writeInt(refIdBytes.length);
        out.write(refIdBytes);

        // matchedReferenceId
        byte[] matchedRefIdBytes = match.getMatchedReferenceId().getBytes(StandardCharsets.UTF_8);
        out.writeInt(matchedRefIdBytes.length);
        out.write(matchedRefIdBytes);

        // compatibility score
        out.writeInt(8);
        out.writeDouble(match.getCompatibilityScore());

        // matchedAt
        BasicUtility.writeTimestamp(
            match.getMatchedAt() != null ? match.getMatchedAt() : DefaultValuesPopulator.getCurrentTimestamp(),
            out
        );
    }
}
