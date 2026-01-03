package com.shedule.x.dto;


import io.minio.*;
import io.minio.errors.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Nonnull;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.Objects;

public class RemoteMultipartFile implements MultipartFile {

    private final String bucketName;
    private final String objectPath;
    private final String originalFileName;
    private final String contentType;
    private final MinioClient minioClient;

    public RemoteMultipartFile(String objectPath, String originalFileName, String contentType, MinioClient minioClient) {
        Objects.requireNonNull(objectPath, "Object path cannot be null");
        Objects.requireNonNull(originalFileName, "Original file name cannot be null");
        Objects.requireNonNull(contentType, "Content type cannot be null");

        this.minioClient = minioClient;;

        // Parse URL or relative path
        ParsedPath parsed = parsePath(objectPath);
        this.bucketName = parsed.bucket();
        this.objectPath = parsed.object();

        this.originalFileName = originalFileName;
        this.contentType = contentType;
    }


    private ParsedPath parsePath(String input) {
        try {
            if (input.startsWith("http")) {
                URI uri = URI.create(input);
                String[] parts = uri.getPath().split("/", 3);

                if (parts.length < 3) {
                    throw new IllegalArgumentException("Invalid MinIO URL: " + input);
                }

                return new ParsedPath(parts[1], parts[2]);
            } else {
                // Use default bucket if relative path is provided
                String bucket = System.getenv("MINIO_BUCKET");
                String object = input;

                if (object.startsWith(bucket + "/")) {
                    object = object.substring(bucket.length() + 1);
                }

                return new ParsedPath(bucket, object);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse MinIO path: " + input, e);
        }
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
            StatObjectResponse stat = minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectPath)
                            .build()
            );
            return stat.size() == 0;
        } catch (ErrorResponseException e) {
            if ("NoSuchKey".equalsIgnoreCase(e.errorResponse().code())) {
                throw new IllegalStateException(
                        "File not found in MinIO — bucket: " + bucketName + ", object: " + objectPath, e);
            }
            throw new IllegalStateException("Error checking file in MinIO: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to check file size in MinIO [bucket=" + bucketName + ", object=" + objectPath + "]", e);
        }
    }

    @Override
    public long getSize() {
        try {
            StatObjectResponse stat = minioClient.statObject(
                    StatObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectPath)
                            .build()
            );
            return stat.size();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to get file size from MinIO [bucket=" + bucketName + ", object=" + objectPath + "]", e);
        }
    }

    @Override
    @Nonnull
    public byte[] getBytes() throws IOException {
        try (InputStream is = getInputStream()) {
            return is.readAllBytes();
        }
    }

    @Override
    @Nonnull
    public InputStream getInputStream() throws IOException {
        try {
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectPath)
                            .build()
            );
        } catch (Exception e) {
            throw new IOException(
                    "Failed to fetch file from MinIO [bucket=" + bucketName + ", object=" + objectPath + "]", e);
        }
    }

    @Override
    public void transferTo(File dest) throws IOException {
        try (InputStream is = getInputStream();
             OutputStream os = new FileOutputStream(dest)) {
            is.transferTo(os);
        }
    }

    @Override
    public void transferTo(java.nio.file.Path dest) throws IOException {
        try (InputStream is = getInputStream()) {
            Files.copy(is, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private record ParsedPath(String bucket, String object) {}
}
