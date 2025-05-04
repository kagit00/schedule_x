package com.shedule.x.utils.basic;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.shedule.x.exceptions.BadRequestException;
import lombok.extern.slf4j.Slf4j;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPInputStream;


@Slf4j
public final class BasicUtility {
    private static final ObjectMapper om = new ObjectMapper();

    static {
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

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

    public static byte[] parseFileContent(Object rawContent) {
        if (rawContent instanceof byte[] c) {
            return decompress(c);
        } else if (rawContent instanceof String s) {
            byte[] decoded = Base64.getDecoder().decode(s);
            return decompress(decoded);
        }
        throw new BadRequestException("Invalid fileContent type: " + rawContent.getClass());
    }

    private static byte[] decompress(byte[] compressedData) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzipIn = new GZIPInputStream(bais);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int len;
            while ((len = gzipIn.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new BadRequestException("Failed to decompress fileContent: " + e.getMessage());
        }
    }

    public static <T> T safeParse(String payload, Class<T> clazz) {
        try {
            return om.readValue(payload, clazz);
        } catch (Exception e) {
            log.debug("Failed to parse payload into {}: {}", clazz.getSimpleName(), payload);
            return null;
        }
    }
}
