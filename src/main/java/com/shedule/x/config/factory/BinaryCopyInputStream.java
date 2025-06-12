package com.shedule.x.config.factory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class BinaryCopyInputStream<T> extends InputStream {
    private final Iterator<T> iterator;
    private final CopyStreamSerializer<T> serializer;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final DataOutputStream out = new DataOutputStream(buffer);
    private byte[] currentBytes;
    private int pos = 0;
    private boolean headerWritten = false;
    private boolean closed = false;

    public BinaryCopyInputStream(List<T> batch, CopyStreamSerializer<T> serializer) {
        this.iterator = batch.iterator();
        this.serializer = serializer;
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
                out.writeInt(0); // flags
                out.writeInt(0); // header extension area length
                headerWritten = true;
            } else if (!iterator.hasNext()) {
                out.writeShort(-1);
                out.flush();
                currentBytes = buffer.toByteArray();
                pos = 0;
                closed = true;
                return currentBytes.length > 0 ? (currentBytes[pos++] & 0xFF) : -1;
            } else {
                T entity = iterator.next();
                serializer.serialize(entity, out);
            }

            out.flush();
            currentBytes = buffer.toByteArray();
            pos = 0;
        }

        return Objects.requireNonNull(currentBytes)[pos++] & 0xFF;
    }

    @Override
    public void close() throws IOException {
        out.close();
        closed = true;
    }
}
