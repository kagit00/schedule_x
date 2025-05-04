package com.shedule.x.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public final class Connector {

    private Connector() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    /**
     * Performs an HTTP request to the specified URL using the provided API key, title,
     * section type, and HTTP method type. The request is sent with JSON content and the
     * appropriate authorization header.
     *
     * @param url the URL to send the request to
     * @param apiKey the API key for authorization
     * @param title the title related to the request (used for generating content)
     * @param sectionType the type of section for which content should be generated
     * @param methodType the HTTP method to use for the request (e.g., GET, POST)
     * @return the raw response body as a string
     */
    public static String performRequest(String url, String apiKey, String title, String sectionType, HttpMethod methodType) {
        String rawResponse = "";
        try {
            RestTemplate restTemplate = new RestTemplate();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("Authorization", "Bearer " + apiKey);
            HttpEntity<Map<String, Object>> entity = getMapHttpEntity(title, sectionType, headers);

            ResponseEntity<String> response = restTemplate.exchange(url, methodType, entity, String.class);

            rawResponse = response.getBody();
            log.info("Raw Response: {}", rawResponse);
        } catch (ResourceAccessException | HttpClientErrorException e) {
            log.error("Error Connecting to the API");
        }
        return rawResponse;
    }

    /**
     * Creates an HttpEntity with a body and headers to send with the request.
     *
     * @param title the title related to the request
     * @param sectionType the type of section for which content should be generated
     * @param headers the HTTP headers to be used
     * @return a configured HttpEntity containing the request body and headers
     */
    private static HttpEntity<Map<String, Object>> getMapHttpEntity(String title, String sectionType, HttpHeaders headers) {
        Map<String, Object> requestBody = new HashMap<>();
        List<Map<String, String>> messages = new ArrayList<>();

        Map<String, String> message = new HashMap<>();
        message.put("role", "user");
        message.put("content", "Generate " + sectionType + " for a resume titled: " + title);
        messages.add(message);

        requestBody.put("messages", messages);
        requestBody.put("model", "mixtral-8x7b-32768");

        return new HttpEntity<>(requestBody, headers);
    }
}
