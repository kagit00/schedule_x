package com.shedule.x.utils.validation;

import com.shedule.x.exceptions.BadRequestException;
import org.springframework.web.multipart.MultipartFile;

public final class FileValidationUtility {

    private FileValidationUtility() {
        throw new UnsupportedOperationException("Unsupported Operation");
    }

    public static void validateInput(MultipartFile file, String groupId) {
        if (file == null || file.isEmpty()) {
            throw new BadRequestException("File cannot be null or empty");
        }
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new BadRequestException("Group ID cannot be null or empty");
        }
    }
}
