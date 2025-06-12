package com.shedule.x.utils.media.praquet;

import com.shedule.x.utils.media.csv.HeaderNormalizer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@UtilityClass
@Slf4j
public final class ParquetSchemaUtil {

    private static final Map<Class<?>, Schema> schemaCache = new HashMap<>();

    public static <T> Schema getSchema(Class<T> clazz, List<ParquetFieldExtractor<T>> extractors) {
        Schema schema = schemaCache.get(clazz);
        if (schema != null) {
            log.debug("Retrieved cached schema for class {}", clazz.getSimpleName());
            return schema;
        }

        try {
            SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                    .record(clazz.getSimpleName())
                    .namespace("com.export.matches")
                    .fields()
                    .name(HeaderNormalizer.FIELD_GROUP_ID)
                    .type()
                    .stringType()
                    .noDefault();

            for (ParquetFieldExtractor<T> extractor : extractors) {
                String hdr = extractor.header();
                Schema fieldType = Schema.create(extractor.avroType());

                fields = fields
                        .name(hdr)
                        .type()
                        .unionOf()
                        .nullType()
                        .and()
                        .type(fieldType)
                        .endUnion()
                        .noDefault();
            }

            schema = fields.endRecord();
            schemaCache.put(clazz, schema);
            log.info("Generated and cached schema for class {}", clazz.getSimpleName());
            return schema;
        } catch (Exception e) {
            log.error("Failed to generate schema for class {}", clazz.getSimpleName(), e);
            throw new IllegalStateException(
                    String.format("Failed to generate schema for class %s: %s",
                            clazz.getSimpleName(), e.getMessage()), e);
        }
    }

    public static <T> GenericRecord createRecord(
            T entity,
            String groupId,
            Schema schema,
            List<ParquetFieldExtractor<T>> extractors
    ) {
        try {
            GenericRecord record = new GenericData.Record(schema);
            record.put(HeaderNormalizer.FIELD_GROUP_ID, groupId);

            for (ParquetFieldExtractor<T> extractor : extractors) {
                record.put(extractor.header(), extractor.extract(entity));
            }

            return record;
        } catch (Exception e) {
            log.error("Failed to create record for entity of type {} with groupId {}",
                    entity.getClass().getSimpleName(), groupId, e);
            throw new IllegalStateException(
                    String.format("Failed to create record for entity of type %s, groupId %s: %s",
                            entity.getClass().getSimpleName(), groupId, e.getMessage()), e);
        }
    }
}