package com.shedule.x.dto;

import jakarta.annotation.Nonnull;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class FileSystemMultipartFile implements MultipartFile {

    private final Path filePath;
    private final String originalFileName;
    private final String contentType;

    public FileSystemMultipartFile(String filePath, String originalFileName, String contentType) {
        if (filePath == null || originalFileName == null || contentType == null) {
            throw new IllegalArgumentException("File path, original file name, and content type cannot be null");
        }
        this.filePath = Paths.get(filePath).toAbsolutePath().normalize();
        this.originalFileName = originalFileName;
        this.contentType = contentType;

        if (!Files.exists(this.filePath)) throw new IllegalArgumentException("File does not exist: " + filePath);
        if (!Files.isReadable(this.filePath)) throw new IllegalArgumentException("File is not readable: " + filePath);
    }

    @Override
    public String getName() {
        return originalFileName;
    }

    @Override
    public String getOriginalFilename() {
        return originalFileName;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public boolean isEmpty() {
        try {
            return Files.size(filePath) == 0;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to check file size: " + filePath, e);
        }
    }

    @Override
    public long getSize() {
        try {
            return Files.size(filePath);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get file size: " + filePath, e);
        }
    }

    @Override
    @Nonnull
    public byte[] getBytes() throws IOException {
        return Files.readAllBytes(filePath);
    }

    @Override
    @Nonnull
    public InputStream getInputStream() throws IOException {
        return Files.newInputStream(filePath);
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
        if (dest == null) {
            throw new IllegalArgumentException("Destination file cannot be null");
        }
        Files.copy(filePath, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void transferTo(Path dest) throws IOException, IllegalStateException {
        if (dest == null) {
            throw new IllegalArgumentException("Destination path cannot be null");
        }
        Files.copy(filePath, dest, StandardCopyOption.REPLACE_EXISTING);
    }
}