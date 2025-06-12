package com.shedule.x.utils.media.csv;

import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.utils.basic.BasicUtility;

import java.util.List;


public final class FieldsExtractor {

    private FieldsExtractor() {
        throw new UnsupportedOperationException("Unsupported");
    }

    public static List<CsvExporter.FieldExtractor<MatchTransfer>> getMatchFieldExtractors() {
        return List.of(
                new CsvExporter.FieldExtractor<>() {
                    @Override
                    public String header() {
                        return HeaderNormalizer.FIELD_REFERENCE_ID;
                    }

                    @Override
                    public String extract(MatchTransfer matchTransfer) {
                        return BasicUtility.safeExtract(matchTransfer.getReferenceId());
                    }
                },
                new CsvExporter.FieldExtractor<>() {
                    @Override
                    public String header() {
                        return "matched_reference_id";
                    }

                    @Override
                    public String extract(MatchTransfer matchTransfer) {
                        return BasicUtility.safeExtract(matchTransfer.getMatchedReferenceId());
                    }
                },
                new CsvExporter.FieldExtractor<>() {
                    @Override
                    public String header() {
                        return "compatibility_score";
                    }

                    @Override
                    public String extract(MatchTransfer matchTransfer) {
                        return BasicUtility.safeExtract(matchTransfer.getCompatibilityScore());
                    }
                },
                new CsvExporter.FieldExtractor<>() {
                    @Override
                    public String header() {
                        return "group_id";
                    }

                    @Override
                    public String extract(MatchTransfer matchTransfer) {
                        return BasicUtility.safeExtract(matchTransfer.getGroupId());
                    }
                },
                new CsvExporter.FieldExtractor<>() {
                    @Override
                    public String header() {
                        return "match_suggestion_type";
                    }

                    @Override
                    public String extract(MatchTransfer matchTransfer) {
                        return BasicUtility.safeExtract(matchTransfer.getMatchSuggestionType());
                    }
                }
        );
    }
}
