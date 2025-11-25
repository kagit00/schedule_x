package com.shedule.x.config.factory;

import com.shedule.x.dto.ChunkTask;
import com.shedule.x.dto.NodeDTO;
import lombok.Getter;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class TaskIterator {
    private final List<List<NodeDTO>> chunks;

    @Getter
    private final int totalTasks;

    public TaskIterator(List<List<NodeDTO>> chunks) {
        this.chunks = chunks;
        int numChunks = chunks.size();
        this.totalTasks = (numChunks * (numChunks + 1)) / 2;
    }


    public ChunkTask getTaskByIndex(int k) {
        int i = (int) ( (Math.sqrt(8L*k + 1) - 1) / 2 );
        while (i*(i+1)/2 > k) i--;
        while ((i+1)*(i+2)/2 <= k) i++;

        int rowStart = (i * (i + 1)) / 2;
        int j = k - rowStart + i;

        return new ChunkTask(i, j, chunks.get(i), chunks.get(j));
    }
}
