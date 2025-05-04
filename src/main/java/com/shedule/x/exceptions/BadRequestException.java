package com.shedule.x.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Custom exception thrown when a bad request is made (HTTP 400).
 * <p>
 * This exception is typically used to signal that the client sent invalid data or parameters in their request.
 * It is annotated with {@link ResponseStatus} to automatically return a HTTP 400 Bad Request status code.
 * </p>
 */
@ResponseStatus(HttpStatus.BAD_REQUEST)
public class BadRequestException extends RuntimeException {

    /**
     * Constructs a new BadRequestException with the specified detail message.
     *
     * @param message the detail message which explains the cause of the exception.
     */
    public BadRequestException(String message) {
        super(message);
    }
}
