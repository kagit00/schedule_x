package com.shedule.x.dto;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public record ExportedFile(byte[] content, String fileName, String contentType, String groupId, UUID domainId, String filePath) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExportedFile that)) return false;
        return Arrays.equals(content, that.content) &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(contentType, that.contentType) &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(domainId, that.domainId) &&
                Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(content), fileName, contentType, groupId, domainId, filePath);
    }

    @Override
    public String toString() {
        return "ExportedFile{content.length=" + (content != null ? content.length : 0) +
                ", fileName='" + fileName + '\'' +
                ", contentType='" + contentType + '\'' +
                ", filePath='" + filePath + '\'' +
                '}';
    }
}