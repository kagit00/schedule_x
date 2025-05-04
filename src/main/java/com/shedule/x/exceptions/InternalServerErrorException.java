package com.shedule.x.exceptions;

/**
 * Exception thrown when an internal server error occurs.
 * <p>
 * This exception is used to indicate that a server error has occurred during the processing
 * of a request, typically due to unexpected conditions or issues that are not related to
 * the client's request.
 * </p>
 */
public class InternalServerErrorException extends RuntimeException {

    /**
     * Constructs a new {@link InternalServerErrorException} with the specified error message.
     *
     * @param m the detail message explaining the error.
     */
    public InternalServerErrorException(String m) {
        super(m);
    }
}
