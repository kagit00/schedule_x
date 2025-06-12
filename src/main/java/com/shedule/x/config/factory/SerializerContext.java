package com.shedule.x.config.factory;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer;
import com.shedule.x.service.GraphRecords;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


public class SerializerContext {

    private static final Set<Class<?>> registeredClasses = new HashSet<>(Arrays.asList(
            UUID.class,
            GraphRecords.PotentialMatch.class
    ));

    private static final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        kryo.register(UUID.class, new UUIDSerializer());
        kryo.register(GraphRecords.PotentialMatch.class, new FieldSerializer<>(kryo, GraphRecords.PotentialMatch.class));
        return kryo;
    });

    public static Kryo get() {
        return kryoThreadLocal.get();
    }

    public static void remove() {
        kryoThreadLocal.remove();
    }

    public static void release() {
        kryoThreadLocal.remove();
    }
}
