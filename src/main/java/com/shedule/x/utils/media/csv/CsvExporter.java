package com.shedule.x.utils.media.csv;

import com.opencsv.CSVWriter;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.exceptions.InternalServerErrorException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;


@Slf4j
public final class CsvExporter {

    private CsvExporter() {
        throw new UnsupportedOperationException("Utility class");
    }

    public interface FieldExtractor<T> {
        String header();
        String extract(T entity);
    }

    public static <T> String exportToCsvString(List<T> entities, String groupIdentifier, List<FieldExtractor<T>> fieldExtractors) {
        if (entities == null) {
            throw new BadRequestException("Entities list cannot be null");
        }
        if (groupIdentifier == null) {
            throw new BadRequestException("Group identifier cannot be null");
        }
        if (fieldExtractors == null || fieldExtractors.isEmpty()) {
            throw new BadRequestException("Field extractors cannot be null or empty");
        }

        String[] headers = fieldExtractors.stream()
                .map(FieldExtractor::header)
                .toArray(String[]::new);

        try (StringWriter stringWriter = new StringWriter();
             CSVWriter writer = new CSVWriter(stringWriter, ',', '"', '"', "\n")) {
            String[] fullHeaders = new String[headers.length + 1];
            fullHeaders[0] = HeaderNormalizer.FIELD_GROUP_ID;
            System.arraycopy(headers, 0, fullHeaders, 1, headers.length);
            writer.writeNext(fullHeaders);

            for (T entity : entities) {
                if (entity == null) {
                    log.warn("Skipping null entity in group '{}'", groupIdentifier);
                    continue;
                }
                writer.writeNext(mapEntityToCsvRow(entity, groupIdentifier, fieldExtractors), false);
            }
            String csvContent = stringWriter.toString();
            log.info("Generated CSV for group '{}', total records: {}", groupIdentifier, entities.size());
            return csvContent;

        } catch (IOException e) {
            log.error("Error generating CSV for group '{}': {}", groupIdentifier, e.getMessage(), e);
            throw new InternalServerErrorException("Failed to export CSV for group " + groupIdentifier + ": " + e.getMessage());
        }
    }

    private static <T> String[] mapEntityToCsvRow(T entity, String groupIdentifier, List<FieldExtractor<T>> fieldExtractors) {
        String[] row = new String[fieldExtractors.size() + 1];
        row[0] = sanitizeValue(groupIdentifier);
        for (int i = 0; i < fieldExtractors.size(); i++) {
            String value = fieldExtractors.get(i).extract(entity);
            row[i + 1] = sanitizeValue(value);
        }
        return row;
    }

    private static String sanitizeValue(String value) {
        if (value == null) {
            return "";
        }
        return "\"" + value.replace("\"", "\"\"").replace("\n", " ").replace("\r", " ") + "\"";
    }
}
