package com.shedule.x.utils.basic;

import com.shedule.x.dto.Error;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.io.PrintWriter;

@Slf4j
public final class ErrorUtility {

    private ErrorUtility() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Gets error.
     *
     * @param errorMsg the error msg
     * @param status   the status
     * @return the error
     */
    public static Error getError(String errorMsg, HttpStatus status) {
        return Error.builder()
                .errorMsg(errorMsg).uid(DefaultValuesPopulator.getUid())
                .status(status).timestamp(DefaultValuesPopulator.getCurrentTimestamp())
                .build();
    }

    /**
     * Print error.
     *
     * @param ex       the ex
     * @param response the response
     */
    public static void printError(String ex, HttpServletResponse response) {
        Error error = getError(ex, HttpStatus.valueOf(response.getStatus()));
        String str = BasicUtility.stringifyObject(error);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        try {
            PrintWriter writer = response.getWriter();
            writer.write(str);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }
}
