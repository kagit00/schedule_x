package com.shedule.x.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Error {
    private String uid;
    private HttpStatus status;
    private LocalDateTime timestamp;
    private String errorMsg;
}
