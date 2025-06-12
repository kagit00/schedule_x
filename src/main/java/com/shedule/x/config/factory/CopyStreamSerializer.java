package com.shedule.x.config.factory;

import java.io.DataOutputStream;
import java.io.IOException;

public interface CopyStreamSerializer<T> {
    void serialize(T entity, DataOutputStream out) throws IOException;
}
