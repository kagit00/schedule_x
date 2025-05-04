package com.shedule.x.service;

import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.utils.media.csv.CsvExporter;
import com.shedule.x.utils.media.csv.FieldsExtractor;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Service
public class ExportServiceImpl implements ExportService {

    @Override
    public ExportedFile exportMatchesAsCsv(List<MatchTransfer> matches, String groupId, UUID domainId) {
        String csv = CsvExporter.exportToCsvString(matches, groupId, FieldsExtractor.getMatchFieldExtractors());
        return new ExportedFile(
                csv.getBytes(StandardCharsets.UTF_8),
                groupId + "_matches.csv",
                "text/csv",
                groupId,
                domainId
        );
    }
}
