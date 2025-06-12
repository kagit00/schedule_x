package com.shedule.x.config.factory;

import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class DelegatingPositionOutputStream extends PositionOutputStream {
    private final OutputStream out;
    private long position = 0;

    public DelegatingPositionOutputStream(OutputStream out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        position++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        position += len;
    }

    @Override
    public long getPos() {
        return position;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
