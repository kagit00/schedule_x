package com.shedule.x.repo;

import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.models.NodesImportJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.UUID;

@Repository
public interface NodesImportJobRepository extends JpaRepository<NodesImportJob, UUID> {

        @Modifying
        @Query("UPDATE NodesImportJob g SET g.status = :status WHERE g.id = :jobId")
        void updateStatus(@Param("jobId") UUID jobId, @Param("status") JobStatus status);

        @Modifying
        @Query("UPDATE NodesImportJob g SET g.totalNodes = :total WHERE g.id = :jobId")
        void updateTotalNodes(@Param("jobId") UUID jobId, @Param("total") int total);

        @Modifying
        @Query("UPDATE NodesImportJob g SET g.processedNodes = g.processedNodes + :count WHERE g.id = :jobId")
        void incrementProcessed(@Param("jobId") UUID jobId, @Param("count") int count);

        @Modifying
        @Query("UPDATE NodesImportJob g SET g.status = :status, g.completedAt = CURRENT_TIMESTAMP WHERE g.id = :jobId")
        void markCompleted(@Param("jobId") UUID jobId, JobStatus status);

        @Modifying
        @Query("UPDATE NodesImportJob g SET g.status = :status, g.errorMessage = :error WHERE g.id = :jobId")
        void markFailed(@Param("jobId") UUID jobId, JobStatus status, @Param("error") String error);

        @Query("SELECT g.processedNodes FROM NodesImportJob g WHERE g.id = :jobId")
        Integer getProcessedNodes(@Param("jobId") UUID jobId);

        @Query("SELECT g.totalNodes FROM NodesImportJob g WHERE g.id = :jobId")
        Integer getTotalNodes(@Param("jobId") UUID jobId);
}

