package com.shedule.x.utils.media.praquet;

import org.apache.avro.Schema;

public interface ParquetFieldExtractor<T> {
    String header();
    Object extract(T entity);
    Schema.Type avroType();
}