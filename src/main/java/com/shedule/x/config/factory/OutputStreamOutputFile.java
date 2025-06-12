package com.shedule.x.config.factory;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamOutputFile implements OutputFile {
    private final OutputStream outputStream;

    public OutputStreamOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new DelegatingPositionOutputStream(outputStream);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}