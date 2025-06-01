package com.shedule.x.utils;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.esotericsoftware.kryo.kryo5.serializers.FieldSerializer;
import com.shedule.x.config.factory.UUIDSerializer;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.service.GraphRecords;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@UtilityClass
@Slf4j
public final class MatchSerializer {
    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(true);
        kryo.register(UUID.class, new UUIDSerializer());
        kryo.register(GraphRecords.PotentialMatch.class, new FieldSerializer<>(kryo, GraphRecords.PotentialMatch.class));
        return kryo;
    });

    public byte[] serialize(GraphRecords.PotentialMatch match) {
        Kryo kryo = kryoThreadLocal.get();
        try (Output output = new Output(128, 1024)) {
            kryo.writeObject(output, match);
            return output.toBytes();
        } catch (Exception e) {
            log.error("Serialization failed: referenceId={}, matchedReferenceId={}",
                    match.getReferenceId(), match.getMatchedReferenceId(), e);
            throw new InternalServerErrorException("Failed to serialize PotentialMatch");
        }
    }

    public GraphRecords.PotentialMatch deserialize(byte[] data, String groupId, UUID domainId) {
        Kryo kryo = kryoThreadLocal.get();
        try (Input input = new Input(data)) {
            GraphRecords.PotentialMatch match = kryo.readObject(input, GraphRecords.PotentialMatch.class);
            return new GraphRecords.PotentialMatch(
                    match.getReferenceId(),
                    match.getMatchedReferenceId(),
                    match.getCompatibilityScore(),
                    groupId,
                    domainId
            );
        } catch (Exception e) {
            log.error("Deserialization failed: groupId={}, domainId={}", groupId, domainId, e);
            throw new InternalServerErrorException("Failed to deserialize PotentialMatch");
        }
    }
}