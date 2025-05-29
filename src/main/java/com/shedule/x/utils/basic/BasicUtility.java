package com.shedule.x.utils.basic;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;


@Slf4j
public final class BasicUtility {
    private static final ObjectMapper om = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);;
    private static final LocalDateTime PG_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    private BasicUtility() {
        throw new UnsupportedOperationException("Not supported");
    }


    public static String stringifyObject(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new com.shedule.x.exceptions.BadRequestException("Failed stringifying object");
        }
    }

    public static String readSpecificProperty(String body, String prop) {
        try {
            return JsonPath.read(body, prop);
        } catch (Exception e) {
            return "";
        }
    }

    public static String getDomainFromUrl(String urlString) {
        try {
            URL url = new URL(urlString);
            return url.getHost();
        } catch (Exception e) {
            return null;
        }
    }

    public static List<String> getStringAsList(String ip, String regex) {
        if (ip != null && !ip.isEmpty()) {
            return Arrays.asList(ip.split(regex));
        } else {
            return List.of();
        }
    }

    public static String formatTimestamp(LocalDateTime timestamp) {
        return timestamp != null ? timestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")) : "";
    }

    public static String safeExtract(Object obj) {
        return obj != null ? obj.toString() : "";
    }


    public static <T> T safeParse(String payload, Class<T> clazz) {
        try {
            return om.readValue(payload, clazz);
        } catch (Exception e) {
            log.debug("Failed to parse payload into {}: {}", clazz.getSimpleName(), payload);
            return null;
        }
    }

    public static <T> T deserializeToBytes(byte[] payload, Class<T> clazz) {
        try {
            return om.readValue(payload, clazz);
        } catch (Exception e) {
            log.debug("Failed to parse payload into {}:.", clazz.getSimpleName());
            return null;
        }
    }

    public static void writeTimestamp(LocalDateTime timestamp, DataOutputStream out) throws IOException {
        if (timestamp == null) {
            log.error("Null timestamp provided for binary COPY");
            throw new IllegalArgumentException("Timestamp cannot be null");
        }
        out.writeInt(8);
        long microsSincePgEpoch = ChronoUnit.MICROS.between(PG_EPOCH, timestamp);
        out.writeLong(microsSincePgEpoch);
    }

}
