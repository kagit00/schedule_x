package com.shedule.x.service;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Node;
import com.shedule.x.processors.LmdbEnvironment;

import com.shedule.x.utils.graph.NodeSerializer;
import com.shedule.x.utils.graph.StoreUtility;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class NodeDataServiceImpl implements NodeDataService {

    private final LmdbEnvironment lmdb;
    private final MeterRegistry meterRegistry;

    private final Map<UUID, int[]> encodedVectorsCache = new ConcurrentHashMap<>();

    public NodeDataServiceImpl(LmdbEnvironment lmdb, MeterRegistry meterRegistry) {
        this.lmdb = lmdb;
        this.meterRegistry = meterRegistry;

        try {
            meterRegistry.gauge("lsh_cache.size", encodedVectorsCache, Map::size);
        } catch (Exception e) {
            log.warn("Unable to register gauge for lsh_cache.size", e);
        }
    }


    @Override
    public void persistNode(NodeDTO node, UUID groupId) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> nodeDbi = lmdb.nodeDbi();

        ByteBuffer keyBuf = StoreUtility.keyBuf();
        ByteBuffer valBuf = StoreUtility.valBuf();

        // Key: [GroupId (16)] + [NodeId (16)]
        StoreUtility.putUUID(keyBuf, groupId);
        StoreUtility.putUUID(keyBuf, node.getId());
        keyBuf.flip();

        // Value: Serialize the NodeDTO object
        NodeSerializer.serialize(node, valBuf);
        valBuf.flip();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            nodeDbi.put(txn, keyBuf, valBuf);
            txn.commit();
            meterRegistry.counter("node_store.written").increment();
        } catch (Exception e) {
            log.error("Failed to persist node {} for group {} to LMDB", node.getId(), groupId, e);
        } finally {
            keyBuf.clear();
            valBuf.clear();
        }
    }


    @Override
    public NodeDTO getNode(UUID nodeId, UUID groupId) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> nodeDbi = lmdb.nodeDbi();

        ByteBuffer keyBuf = StoreUtility.keyBuf();
        StoreUtility.putUUID(keyBuf, groupId);
        StoreUtility.putUUID(keyBuf, nodeId);
        keyBuf.flip();

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer val = nodeDbi.get(txn, keyBuf);
            if (val == null) {
                return null;
            }
            return NodeSerializer.deserialize(val);
        } catch (Exception e) {
            log.error("Failed to retrieve node {} for group {} from LMDB", nodeId, groupId, e);
            return null;
        } finally {
            keyBuf.clear();
        }
    }

    @Override
    public Map<UUID, int[]> getEncodedVectorsCache(UUID groupId) {
        return encodedVectorsCache;
    }

    @Override
    public void updateEncodedVectorsCache(Map<UUID, int[]> newVectors) {
        if (newVectors == null || newVectors.isEmpty()) return;
        encodedVectorsCache.putAll(newVectors);
        log.info("LSH encoded vector cache updated. Size: {}", encodedVectorsCache.size());
    }

    @Override
    public void cleanup(UUID groupId) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> nodeDbi = lmdb.nodeDbi();

        // Prepare prefix buffer containing only the groupId bytes
        ByteBuffer prefix = StoreUtility.keyBuf();
        prefix.clear();
        StoreUtility.putUUID(prefix, groupId);
        prefix.flip();

        final List<UUID> nodesToDelete = new ArrayList<>();

        // ---------------- PHASE 1: SCAN (read-only txn + cursor) ----------------
        try (Txn<ByteBuffer> rtxn = env.txnRead();
             Cursor<ByteBuffer> cursor = nodeDbi.openCursor(rtxn)) {

            // Position cursor at first key >= prefix using GetOp.MDB_SET_RANGE
            boolean found = cursor.get(prefix, GetOp.MDB_SET_RANGE);

            while (found) {
                ByteBuffer keyBuf = cursor.key();

                // Stop if key no longer starts with this groupId prefix
                if (!StoreUtility.keyStartsWith(keyBuf, prefix)) {
                    break;
                }

                // Extract nodeId from key: skip 16 bytes of groupId
                ByteBuffer dup = keyBuf.duplicate();
                dup.position(16);
                UUID nodeId = StoreUtility.readUUIDFromBuffer(dup);

                nodesToDelete.add(nodeId);
                found = cursor.next();
            }

        } catch (Exception e) {
            log.error("LMDB scan failed for group {}", groupId, e);
            return;
        }

        // Remove found ids from in-memory cache
        for (UUID id : nodesToDelete) {
            encodedVectorsCache.remove(id);
        }
        log.info("Cleared {} entries from in-memory vector cache for group {}", nodesToDelete.size(), groupId);

        // ---------------- PHASE 2: DELETE FROM LMDB (write txn) ----------------
        if (!nodesToDelete.isEmpty()) {
            try (Txn<ByteBuffer> wtxn = env.txnWrite()) {
                for (UUID nodeId : nodesToDelete) {
                    ByteBuffer key = StoreUtility.keyBuf();
                    key.clear();
                    StoreUtility.putUUID(key, groupId);
                    StoreUtility.putUUID(key, nodeId);
                    key.flip();

                    // Use Dbi.delete(txn, key)
                    nodeDbi.delete(wtxn, key);
                }
                wtxn.commit();
                log.info("Deleted {} nodes from LMDB for group {}", nodesToDelete.size(), groupId);
            } catch (Exception e) {
                log.error("LMDB deletion failed for group {}", groupId, e);
            }
        } else {
            log.info("No nodes to delete for group {}", groupId);
        }
    }

    @Override
    public NodeDTO getNode(UUID nodeId) {
        throw new UnsupportedOperationException("Use getNode(UUID nodeId, UUID groupId) for LMDB access.");
    }
}
