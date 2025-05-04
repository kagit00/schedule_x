package com.shedule.x.dto;

import org.springframework.lang.NonNull;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;

public class BytesMultipartFile implements MultipartFile {

    private final byte[] content;
    private final String originalFilename;
    private final String contentType;

    public BytesMultipartFile(String originalFilename, String contentType, byte[] content) {
        this.originalFilename = originalFilename;
        this.contentType = contentType;
        this.content = content;
    }

    @Override @NonNull public String getName() { return "file"; }
    @Override public String getOriginalFilename() { return originalFilename; }
    @Override public String getContentType() { return contentType; }
    @Override public boolean isEmpty() { return content.length == 0; }
    @Override public long getSize() { return content.length; }
    @Override @NonNull public byte[] getBytes() { return content; }
    @Override @NonNull public InputStream getInputStream() { return new ByteArrayInputStream(content); }
    @Override public void transferTo(File dest) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(dest)) {
            fos.write(content);
        }
    }
}
