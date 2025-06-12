package com.shedule.x.service;

import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface ExportService {
    CompletableFuture<ExportedFile> exportMatches(Supplier<Stream<MatchTransfer>> matchesSupplier, String groupId, UUID domainId);
}
