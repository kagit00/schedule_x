package com.shedule.x.config;

import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.utils.media.csv.HeaderNormalizer;
import com.shedule.x.utils.media.praquet.ParquetFieldExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.List;

@Configuration
@Slf4j
class MatchFieldExtractors {

    @Bean
    public List<ParquetFieldExtractor<MatchTransfer>> fieldExtractors() {
        return List.of(
                new ParquetFieldExtractor<MatchTransfer>() {
                    @Override
                    public String header() {
                        return HeaderNormalizer.FIELD_REFERENCE_ID;
                    }

                    @Override
                    public Object extract(MatchTransfer entity) {
                        if (entity.getReferenceId() == null) {
                            log.warn("Null referenceId for MatchTransfer entity");
                            return null;
                        }
                        return entity.getReferenceId();
                    }

                    @Override
                    public Schema.Type avroType() {
                        return Schema.Type.STRING;
                    }
                },
                new ParquetFieldExtractor<MatchTransfer>() {
                    @Override
                    public String header() {
                        return "matched_reference_id";
                    }

                    @Override
                    public Object extract(MatchTransfer entity) {
                        if (entity.getMatchedReferenceId() == null) {
                            log.warn("Null matchedReferenceId for MatchTransfer entity");
                            return null;
                        }
                        return entity.getMatchedReferenceId();
                    }

                    @Override
                    public Schema.Type avroType() {
                        return Schema.Type.STRING;
                    }
                },
                new ParquetFieldExtractor<MatchTransfer>() {
                    @Override
                    public String header() {
                        return "compatibility_score";
                    }

                    @Override
                    public Object extract(MatchTransfer entity) {
                        return entity.getCompatibilityScore();
                    }

                    @Override
                    public Schema.Type avroType() {
                        return Schema.Type.DOUBLE;
                    }
                },
                new ParquetFieldExtractor<MatchTransfer>() {
                    @Override
                    public String header() {
                        return "match_suggestion_type";
                    }

                    @Override
                    public Object extract(MatchTransfer entity) {
                        if (entity.getMatchSuggestionType() == null) {
                            log.warn("Null matchSuggestionType for MatchTransfer entity");
                            return null;
                        }
                        return entity.getMatchSuggestionType();
                    }

                    @Override
                    public Schema.Type avroType() {
                        return Schema.Type.STRING;
                    }
                }
        );
    }
}