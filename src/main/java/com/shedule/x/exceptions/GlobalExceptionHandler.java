package com.shedule.x.exceptions;

import com.shedule.x.models.Error;
import com.shedule.x.utils.basic.ErrorUtility;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.util.NoSuchElementException;


/**
 * Global exception handler for handling various exceptions in the application.
 * <p>
 * This class is annotated with {@link RestControllerAdvice} to globally handle exceptions thrown by any controller.
 * Each exception type is mapped to a specific HTTP status code and custom error message.
 * </p>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handles {@link BadRequestException} and returns a HTTP 400 Bad Request response with the error details.
     *
     * @param e the {@link BadRequestException} to be handled.
     * @return a {@link ResponseEntity} containing the error details and a HTTP 400 status.
     */
    @ExceptionHandler(BadRequestException.class)
    public ResponseEntity<Error> handleBadRequestException(BadRequestException e) {
        return new ResponseEntity<>(ErrorUtility.getError(e.getMessage(), HttpStatus.BAD_REQUEST), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles {@link MethodArgumentNotValidException} and returns a HTTP 400 Bad Request response with validation errors.
     *
     * @param ex the {@link MethodArgumentNotValidException} to be handled.
     * @return a {@link ResponseEntity} containing the validation errors and a HTTP 400 status.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Error> handleValidationException(MethodArgumentNotValidException ex) {
        BindingResult bindingResult = ex.getBindingResult();
        StringBuilder errorMessage = new StringBuilder("Invalid request parameters:");

        for (FieldError fieldError : bindingResult.getFieldErrors()) {
            errorMessage.append(" Field '").append(fieldError.getField())
                    .append("' ").append(fieldError.getDefaultMessage()).append("; ");
        }
        return new ResponseEntity<>(ErrorUtility.getError(errorMessage.toString(), HttpStatus.BAD_REQUEST), HttpStatus.BAD_REQUEST);
    }

    /**
     * Handles {@link InternalServerErrorException} and returns a HTTP 500 Internal Server Error response with the error details.
     *
     * @param e the {@link InternalServerErrorException} to be handled.
     * @return a {@link ResponseEntity} containing the error details and a HTTP 500 status.
     */
    @ExceptionHandler(InternalServerErrorException.class)
    public ResponseEntity<Error> handleInternalServerErrorException(InternalServerErrorException e) {
        return new ResponseEntity<>(ErrorUtility.getError(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR), HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Handles {@link NoSuchElementException} and returns a HTTP 400 Bad Request response with the error details.
     *
     * @param e the {@link NoSuchElementException} to be handled.
     * @return a {@link ResponseEntity} containing the error details and a HTTP 400 status.
     */
    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Error> handleNoSuchElementException(NoSuchElementException e) {
        return new ResponseEntity<>(ErrorUtility.getError(e.getMessage(), HttpStatus.BAD_REQUEST), HttpStatus.BAD_REQUEST);
    }
}

