package com.shedule.x.service;

import com.shedule.x.dto.NodeExchange;


public interface ImportJobService {
    void startNodesImport(NodeExchange payload);
}
