package com.shedule.x.config;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class LSHConfig {
    private final int numHashTables;
    private final int numBands;
    private final int topK;
}