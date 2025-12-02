package com.shedule.x.processors;

import com.shedule.x.config.factory.CopyStreamSerializer;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import lombok.extern.slf4j.Slf4j;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Slf4j
public class PerfectMatchSerializer implements CopyStreamSerializer<PerfectMatchEntity> {

    private final UUID groupId;
    private final UUID domainId;
    private final String processingCycleId;

    public PerfectMatchSerializer(List<PerfectMatchEntity> batch,
                                  UUID groupId,
                                  UUID domainId,
                                  String processingCycleId) {
        this.groupId = groupId;
        this.domainId = domainId;
        this.processingCycleId = (processingCycleId != null) ? processingCycleId : "";
    }

    @Override
    public void serialize(PerfectMatchEntity match, DataOutputStream out) throws IOException {

        out.writeShort(8); // exactly 8 columns

        UUID id = match.getId() != null ? match.getId() : DefaultValuesPopulator.getUid2();
        out.writeInt(16);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());

        out.writeInt(16);
        out.writeLong(groupId.getMostSignificantBits());
        out.writeLong(groupId.getLeastSignificantBits());

        out.writeInt(16);
        out.writeLong(domainId.getMostSignificantBits());
        out.writeLong(domainId.getLeastSignificantBits());

        byte[] cycleBytes = processingCycleId.getBytes(StandardCharsets.UTF_8);
        out.writeInt(cycleBytes.length);
        out.write(cycleBytes);

        // 5. reference_id (VARCHAR(50))
        String refId = match.getReferenceId();
        byte[] refBytes = (refId != null ? refId : "").getBytes(StandardCharsets.UTF_8);
        out.writeInt(refBytes.length);
        out.write(refBytes);

        // 6. matched_reference_id (VARCHAR(50))
        String matchedRefId = match.getMatchedReferenceId();
        byte[] matchedRefBytes = (matchedRefId != null ? matchedRefId : "").getBytes(StandardCharsets.UTF_8);
        out.writeInt(matchedRefBytes.length);
        out.write(matchedRefBytes);

        // 7. compatibility_score (DOUBLE PRECISION)
        out.writeInt(8);
        out.writeDouble(match.getCompatibilityScore());

        // 8. matched_at (TIMESTAMP)
        LocalDateTime matchedAt = match.getMatchedAt() != null
                ? match.getMatchedAt()
                : DefaultValuesPopulator.getCurrentTimestamp();
        BasicUtility.writeTimestamp(matchedAt, out);
    }
}
