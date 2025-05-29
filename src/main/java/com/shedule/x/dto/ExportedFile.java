package com.shedule.x.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

@AllArgsConstructor
@Data
public class ExportedFile {
    private final byte[] content;
    private final String fileName;
    private final String contentType;
    private final String groupId;
    private final UUID domainId;
    private final String filePath;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExportedFile)) return false;
        ExportedFile that = (ExportedFile) o;
        return Arrays.equals(content, that.content) &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(contentType, that.contentType) &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(domainId, that.domainId) &&
                Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fileName, contentType, groupId, domainId, filePath);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }

    @Override
    public String toString() {
        return "ExportedFile{" +
                "content.length=" + (content != null ? content.length : 0) +
                ", fileName='" + fileName + '\'' +
                ", contentType='" + contentType + '\'' +
                ", filePath='" + filePath + '\'' +
                '}';
    }
}
