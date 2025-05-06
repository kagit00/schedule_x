package com.shedule.x.service;

import com.opencsv.CSVWriter;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.media.csv.CsvExporter;
import com.shedule.x.utils.media.csv.FieldsExtractor;
import com.shedule.x.utils.media.csv.HeaderNormalizer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

@Service
@Slf4j
public class ExportServiceImpl implements ExportService {

    @Override
    public ExportedFile exportMatchesAsCsv(List<MatchTransfer> matches, String groupId, UUID domainId) {
        try {
            String baseDir = "e:/matches/";
            Files.createDirectories(Paths.get(baseDir, domainId.toString(), groupId));
            String fileName = groupId + "_matches_batch_" + DefaultValuesPopulator.getUid() + ".csv.gz";
            Path fullPath = Paths.get(baseDir, domainId.toString(), groupId, fileName);

            try (Writer writer = new OutputStreamWriter(
                    new GZIPOutputStream(Files.newOutputStream(fullPath)),
                    StandardCharsets.UTF_8
            )) {
                CSVWriter csvWriter = new CSVWriter(writer, ',', '"', '"', "\n");

                List<CsvExporter.FieldExtractor<MatchTransfer>> fieldExtractors = FieldsExtractor.getMatchFieldExtractors();
                String[] headers = new String[fieldExtractors.size() + 1];
                headers[0] = HeaderNormalizer.FIELD_GROUP_ID;
                for (int i = 0; i < fieldExtractors.size(); i++) {
                    headers[i + 1] = fieldExtractors.get(i).header();
                }

                csvWriter.writeNext(headers);
                for (MatchTransfer match : matches) {
                    csvWriter.writeNext(CsvExporter.mapEntityToCsvRow(match, groupId, fieldExtractors), false);
                }

                log.info("Exported {} match rows to {}", matches.size(), fullPath);
            }
            return new ExportedFile(null, fileName, "application/gzip", groupId, domainId, fullPath.toString());

        } catch (IOException e) {
            log.error("Error writing compressed CSV for group '{}'", groupId, e);
            throw new InternalServerErrorException("Failed to export to disk: " + e.getMessage());
        }
    }
}
