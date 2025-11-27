package com.shedule.x.processors;

import com.shedule.x.service.GraphRecords;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class SegmentedDiskBuffer implements AutoCloseable {
        private final Path directory;
        private final int itemsPerSegment;
        private final AtomicLong totalSize = new AtomicLong(0);

        private long currentWriteSegmentId = 0;
        private long currentReadSegmentId = 0;

        private ObjectOutputStream currentWriter;
        private ObjectInputStream currentReader;

        private boolean isWriterFresh = true;

        public SegmentedDiskBuffer(Path directory, int itemsPerSegment) throws IOException {
            this.directory = directory;
            this.itemsPerSegment = itemsPerSegment;
            Files.createDirectories(directory);
            recoverState();
        }

        private void recoverState() throws IOException {
            cleanDirectory();
            openWriter();
        }

        private void cleanDirectory() throws IOException {
            try (Stream<Path> files = Files.list(directory)) {
                files.forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
            }
        }

        public synchronized void write(GraphRecords.PotentialMatch match) throws IOException {
            // Rotate file if too large
            if (Files.size(getSegmentPath(currentWriteSegmentId)) > itemsPerSegment * 1024L) { 
                rotateWriter();
            }

            currentWriter.writeObject(match);
            // reset to prevent memory leaks in ObjectOutputStream reference cache
            currentWriter.reset();
            totalSize.incrementAndGet();
        }

        private Path getSegmentPath(long id) {
            return directory.resolve(String.format("segment_%d.bin", id));
        }

        private void openWriter() throws IOException {
            Path p = getSegmentPath(currentWriteSegmentId);
            boolean exists = Files.exists(p);
            FileOutputStream fos = new FileOutputStream(p.toFile(), true);
            BufferedOutputStream bos = new BufferedOutputStream(fos);

            if (exists) {
                currentWriter = new AppendingObjectOutputStream(bos);
            } else {
                currentWriter = new ObjectOutputStream(bos);
            }
        }

        private void rotateWriter() throws IOException {
            currentWriter.close();
            currentWriteSegmentId++;
            openWriter();
        }

        public synchronized void readBatch(List<GraphRecords.PotentialMatch> batch, int limit) throws IOException {
            if (totalSize.get() == 0) return;

            int count = 0;
            while (count < limit && totalSize.get() > 0) {
                if (currentReader == null) {
                    Path p = getSegmentPath(currentReadSegmentId);
                    if (!Files.exists(p)) {
                        if (currentReadSegmentId < currentWriteSegmentId) {
                            currentReadSegmentId++;
                            continue;
                        } else {
                            break;
                        }
                    }
                    currentReader = new ObjectInputStream(new BufferedInputStream(new FileInputStream(p.toFile())));
                }

                try {
                    GraphRecords.PotentialMatch m = (GraphRecords.PotentialMatch) currentReader.readObject();
                    batch.add(m);
                    count++;
                    totalSize.decrementAndGet();
                } catch (EOFException e) {
                    currentReader.close();
                    currentReader = null;
                    Files.deleteIfExists(getSegmentPath(currentReadSegmentId)); // Delete processed file
                    currentReadSegmentId++;
                } catch (ClassNotFoundException e) {
                    throw new IOException("Class mismatch", e);
                }
            }
        }

        public long size() { return totalSize.get(); }

        @Override
        public synchronized void close() throws IOException {
            if (currentWriter != null) currentWriter.close();
            if (currentReader != null) currentReader.close();
        }

    private static class AppendingObjectOutputStream extends ObjectOutputStream {
        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }
        @Override
        protected void writeStreamHeader() throws IOException {
            reset();
        }
    }
}
