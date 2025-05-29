package com.shedule.x.processors;

import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.DefaultValuesPopulator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


public class BinaryCopyInputStream extends InputStream {
        private final Iterator<PotentialMatchEntity> iterator;
        private final String groupId;
        private final UUID domainId;
        private final String processingCycleId;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private DataOutputStream out = new DataOutputStream(buffer);
        private byte[] currentBytes;
        private int pos = 0;
        private boolean headerWritten = false;
        private boolean closed = false;

        public BinaryCopyInputStream(List<PotentialMatchEntity> batch, String groupId, UUID domainId, String processingCycleId) {
            this.iterator = batch.iterator();
            this.groupId = groupId;
            this.domainId = domainId;
            this.processingCycleId = processingCycleId != null ? processingCycleId : "";
        }

        @Override
        public int read() throws IOException {
            while (pos >= (currentBytes != null ? currentBytes.length : 0)) {
                if (closed) {
                    return -1;
                }
                buffer.reset();
                if (!headerWritten) {
                    out.writeBytes("PGCOPY\n\377\r\n\0");
                    out.writeInt(0);
                    out.writeInt(0);
                    headerWritten = true;
                } else if (!iterator.hasNext()) {
                    out.writeShort(-1);
                    out.flush();
                    currentBytes = buffer.toByteArray();
                    pos = 0;
                    closed = true;
                    return currentBytes.length > 0 ? (currentBytes[pos++] & 0xFF) : -1;
                } else {
                    PotentialMatchEntity match = iterator.next();
                    out.writeShort(8);
                    out.writeInt(16);
                    UUID id = match.getId() == null? DefaultValuesPopulator.getUid2() : match.getId();
                    out.writeLong(id.getMostSignificantBits());
                    out.writeLong(id.getLeastSignificantBits());
                    byte[] groupIdBytes = groupId.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(groupIdBytes.length);
                    out.write(groupIdBytes);
                    out.writeInt(16);
                    out.writeLong(domainId.getMostSignificantBits());
                    out.writeLong(domainId.getLeastSignificantBits());
                    byte[] cycleIdBytes = processingCycleId.getBytes(StandardCharsets.UTF_8);
                    out.writeInt(cycleIdBytes.length);
                    out.write(cycleIdBytes);
                    byte[] refIdBytes = match.getReferenceId().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(refIdBytes.length);
                    out.write(refIdBytes);
                    byte[] matchedRefIdBytes = match.getMatchedReferenceId().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(matchedRefIdBytes.length);
                    out.write(matchedRefIdBytes);
                    out.writeInt(8);
                    out.writeDouble(match.getCompatibilityScore());
                    BasicUtility.writeTimestamp(match.getMatchedAt() != null ? match.getMatchedAt() : DefaultValuesPopulator.getCurrentTimestamp(), out);
                }
                out.flush();
                currentBytes = buffer.toByteArray();
                pos = 0;
            }
            return currentBytes[pos++] & 0xFF;
        }

        @Override
        public void close() throws IOException {
            out.close();
            closed = true;
        }
}
