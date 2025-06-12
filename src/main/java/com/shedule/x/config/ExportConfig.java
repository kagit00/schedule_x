package com.shedule.x.config;

import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Component
@ConfigurationProperties(prefix = "export")
@Getter
@Setter
public class ExportConfig {

    private String baseDir = "e:/matches/";
    private String compressionCodec = "SNAPPY";
    @Min(1)
    private long rowGroupSize = 134217728; // 128MB
    @Min(1)
    private int retryCount = 3;
    @Min(1)
    private int semaphoreLimit = 5;

    public CompressionCodecName getCompressionCodec() {
        return CompressionCodecName.valueOf(compressionCodec);
    }
}