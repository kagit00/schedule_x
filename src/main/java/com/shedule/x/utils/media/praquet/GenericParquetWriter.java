package com.shedule.x.utils.media.praquet;

import com.shedule.x.config.ExportConfig;
import com.shedule.x.config.factory.OutputStreamOutputFile;
import lombok.experimental.UtilityClass;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@UtilityClass
public class GenericParquetWriter {

    public static <T> void writeParquetFile(
            ExportConfig exportConfig,
            Path outputPath,
            Stream<T> records,
            Schema schema,
            BiFunction<T, Schema, GenericRecord> recordMapper
    ) throws IOException {

        try (OutputStream out = Files.newOutputStream(outputPath);
             ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new OutputStreamOutputFile(out))
                     .withSchema(schema)
                     .withCompressionCodec(exportConfig.getCompressionCodec())
                     .withRowGroupSize(exportConfig.getRowGroupSize())
                     .withPageSize(ParquetProperties.DEFAULT_PAGE_SIZE)
                     .withConf(new Configuration(false))
                     .build()
        ) {
            records.forEach(record -> {
                try {
                    writer.write(recordMapper.apply(record, schema));
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to write record to Parquet", e);
                }
            });
        }
    }
}
