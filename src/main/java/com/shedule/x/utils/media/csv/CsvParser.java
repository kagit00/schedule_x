package com.shedule.x.utils.media.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.config.factory.ResponseFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public final class CsvParser {

    private static final int BATCH_SIZE = 50000;

    private CsvParser() {
        throw new UnsupportedOperationException("Unsupported");
    }

    public static <T> void parseInBatches(InputStream inputStream, ResponseFactory<T> responseFactory, Consumer<List<T>> batchConsumer) {
        try (InputStreamReader reader = new InputStreamReader(inputStream);
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withSkipLines(0)
                     .withKeepCarriageReturn(false)
                     .build()) {

            String[] headers = csvReader.readNext();
            Map<String, Integer> headerMap = validateAndNormalizeHeaders(headers);

            parseRows(csvReader, headerMap, responseFactory, batchConsumer);

        } catch (IOException | CsvValidationException e) {
            log.error("Error parsing CSV: {}", e.getMessage(), e);
            throw new InternalServerErrorException("Error parsing CSV: " + e.getMessage());
        }
    }

    private static Map<String, Integer> validateAndNormalizeHeaders(String[] headers) {
        if (headers == null || headers.length == 0) {
            log.error("CSV has no headers");
            throw new BadRequestException("CSV must have headers");
        }

        Map<String, Integer> headerMap = HeaderNormalizer.normalizeHeaders(headers);

        boolean hasRefId = headerMap.containsKey(HeaderNormalizer.FIELD_REFERENCE_ID);
        boolean hasGroupId = headerMap.containsKey(HeaderNormalizer.FIELD_GROUP_ID);

        if (!hasRefId || !hasGroupId) {
            log.error("CSV missing required headers: reference_id={}, group_id={}. Raw headers: {}",
                    hasRefId ? "present" : "missing",
                    hasGroupId ? "present" : "missing",
                    Arrays.toString(headers));
            throw new BadRequestException("CSV must contain reference_id and group_id headers");
        }

        log.debug("Normalized headers: {}", headerMap.keySet());
        return headerMap;
    }

    private static <T> void parseRows(CSVReader csvReader, Map<String, Integer> headerMap,
                                      ResponseFactory<T> responseFactory, Consumer<List<T>> batchConsumer) {
        try {
            String[] csvRow;
            int rowIndex = 0;
            int parsedCount = 0;
            int skippedCount = 0;

            List<T> batch = new ArrayList<>(BATCH_SIZE);

            while ((csvRow = csvReader.readNext()) != null) {
                rowIndex++;
                T parsed = safelyParseRow(headerMap, csvRow, rowIndex, responseFactory);

                if (parsed != null) {
                    batch.add(parsed);
                    parsedCount++;
                    if (batch.size() == BATCH_SIZE) {
                        batchConsumer.accept(new ArrayList<>(batch));
                        batch.clear();
                    }
                } else {
                    skippedCount++;
                }
            }

            if (!batch.isEmpty()) {
                batchConsumer.accept(new ArrayList<>(batch));
            }

            log.info("CSV parsing completed. Total rows: {}, Parsed: {}, Skipped: {}", rowIndex, parsedCount, skippedCount);

        } catch (CsvValidationException e) {
            throw new BadRequestException(e.getMessage());
        } catch (IOException e) {
            throw new InternalServerErrorException(e.getMessage());
        }
    }


    private static <T> T safelyParseRow(Map<String, Integer> headerMap, String[] row, int rowIndex, ResponseFactory<T> responseFactory) {
        try {
            return processCsvRow(headerMap, row, rowIndex, responseFactory);
        } catch (Exception e) {
            log.warn("Skipping row {} due to parsing error: {}. Row data: {}", rowIndex, e.getMessage(), Arrays.toString(row));
            return null;
        }
    }

    private static <T> T processCsvRow(Map<String, Integer> headerMap, String[] rawRow, int rowIndex, ResponseFactory<T> responseFactory) {
        Map<String, String> metadata = new HashMap<>();
        Map<String, String> specialFields = extractSpecialFields(headerMap, rawRow, rowIndex, metadata);

        String referenceId = specialFields.get(HeaderNormalizer.FIELD_REFERENCE_ID);
        String groupId = specialFields.get(HeaderNormalizer.FIELD_GROUP_ID);
        NodeType type = specialFields.get(HeaderNormalizer.FIELD_TYPE) != null
                ? parseNodeType(specialFields.get(HeaderNormalizer.FIELD_TYPE), rowIndex)
                : NodeType.USER;

        if (isRequiredFieldMissing(referenceId, groupId, rowIndex, rawRow)) {
            return null;
        }

        if (type == NodeType.USER && specialFields.get(HeaderNormalizer.FIELD_TYPE) == null) {
            log.debug("Row {}: Type not specified, defaulting to {}", rowIndex, type);
        }

        log.debug("Row {}: referenceId={}, groupId={}, metadata={}", rowIndex, referenceId, groupId, metadata);
        return responseFactory.createResponse(type, referenceId, metadata, groupId);
    }

    private static Map<String, String> extractSpecialFields(Map<String, Integer> headerMap, String[] rawRow, int rowIndex, Map<String, String> metadata) {
        Map<String, String> specialFields = new HashMap<>();
        specialFields.put(HeaderNormalizer.FIELD_REFERENCE_ID, null);
        specialFields.put(HeaderNormalizer.FIELD_GROUP_ID, null);
        specialFields.put(HeaderNormalizer.FIELD_TYPE, null);

        for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
            String header = entry.getKey();
            String value = extractValue(rawRow, entry.getValue());

            if (isSpecialField(header)) {
                if (specialFields.containsKey(header)) {
                    specialFields.put(header, value.isEmpty() ? null : value);
                } else {
                    log.warn("Row {}: Unexpected special field '{}'", rowIndex, header);
                }
            } else {
                metadata.put(header, value);
            }
        }

        return specialFields;
    }

    private static String extractValue(String[] row, int index) {
        String value = (index < row.length && row[index] != null) ? row[index] : "";
        return ValueSanitizer.sanitize(value);
    }

    private static boolean isSpecialField(String header) {
        return header.equals(HeaderNormalizer.FIELD_TYPE)
                || header.equals(HeaderNormalizer.FIELD_REFERENCE_ID)
                || header.equals(HeaderNormalizer.FIELD_GROUP_ID);
    }

    private static NodeType parseNodeType(String value, int rowIndex) {
        if (value.isEmpty()) return null;
        try {
            return NodeType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Row {}: Unknown NodeType [{}]. Skipping type assignment.", rowIndex, value);
            return null;
        }
    }

    private static boolean isRequiredFieldMissing(String referenceId, String groupId, int rowIndex, String[] rawRow) {
        if (!RowValidator.isValid(referenceId, groupId)) {
            log.warn("Row {}: Missing required fields (reference_id={}, group_id={}). Row data: {}",
                    rowIndex, referenceId, groupId, Arrays.toString(rawRow));
            return true;
        }
        return false;
    }
}
