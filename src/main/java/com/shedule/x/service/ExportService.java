package com.shedule.x.service;

import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;

import java.util.List;
import java.util.UUID;

public interface ExportService {
    ExportedFile exportMatchesAsCsv(List<MatchTransfer> matches, String groupId, UUID domainId);
}
