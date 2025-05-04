package com.shedule.x.dto;

import lombok.*;
import org.springframework.http.HttpStatus;

/**
 * The type No content.
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class NoContent {
    private HttpStatus status;
    private String msg;
}
