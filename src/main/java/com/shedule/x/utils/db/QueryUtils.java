package com.shedule.x.utils.db;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class QueryUtils {

    private static final String UPSERT_POTENTIAL_MATCHES_SQL =
            "INSERT INTO public.potential_matches (\n" +
                    "    id,\n" +
                    "    group_id,\n" +
                    "    domain_id,\n" +
                    "    processing_cycle_id,\n" +
                    "    reference_id,\n" +
                    "    matched_reference_id,\n" +
                    "    compatibility_score,\n" +
                    "    matched_at\n" +
                    ")\n" +
                    "SELECT\n" +
                    "    id,\n" +
                    "    group_id,\n" +
                    "    domain_id,\n" +
                    "    processing_cycle_id,\n" +
                    "    reference_id,\n" +
                    "    matched_reference_id,\n" +
                    "    compatibility_score,\n" +
                    "    matched_at\n" +
                    "FROM (\n" +
                    "    SELECT DISTINCT ON (match_id)\n" +
                    "           id,\n" +
                    "           group_id,\n" +
                    "           domain_id,\n" +
                    "           processing_cycle_id,\n" +
                    "           reference_id,\n" +
                    "           matched_reference_id,\n" +
                    "           compatibility_score,\n" +
                    "           matched_at,\n" +
                    "           LEAST(reference_id, matched_reference_id) || '_' || GREATEST(reference_id, matched_reference_id) AS match_id,\n" +
                    "           ROW_NUMBER() OVER (\n" +
                    "               PARTITION BY group_id, reference_id\n" +
                    "               ORDER BY compatibility_score DESC\n" +
                    "           ) AS rn\n" +
                    "    FROM temp_potential_matches\n" +
                    "    WHERE group_id = ?\n" +
                    "      AND processing_cycle_id = ?\n" +
                    ") ranked\n" +
                    "WHERE rn <= 500\n" +
                    "ON CONFLICT (group_id, reference_id, matched_reference_id)\n" +
                    "DO UPDATE SET\n" +
                    "    compatibility_score = EXCLUDED.compatibility_score,\n" +
                    "    matched_at = EXCLUDED.matched_at";

    private static final String POTENTIAL_MATCHES_DROP_TEMP_TABLE_SQL =
            "DROP TABLE IF EXISTS temp_potential_matches";

    private static final String POTENTIAL_MATCHES_TEMP_TABLE_SQL =
            "CREATE TEMPORARY TABLE IF NOT EXISTS temp_potential_matches (\n" +
                    "    id UUID NOT NULL,\n" +
                    "    group_id UUID,\n" +
                    "    domain_id UUID,\n" +
                    "    processing_cycle_id VARCHAR(50),\n" +
                    "    reference_id VARCHAR(50),\n" +
                    "    matched_reference_id VARCHAR(50),\n" +
                    "    compatibility_score DOUBLE PRECISION,\n" +
                    "    matched_at TIMESTAMP\n" +
                    ") ";

    public static String getUpsertPotentialMatchesSql() {
        return UPSERT_POTENTIAL_MATCHES_SQL;
    }

    public static String getPotentialMatchesDropTempTableSql() {
        return POTENTIAL_MATCHES_DROP_TEMP_TABLE_SQL;
    }

    public static String getPotentialMatchesTempTableSql() {
        return POTENTIAL_MATCHES_TEMP_TABLE_SQL;
    }

    public static String getAllPotentialMatchesStreamingSQL() {
        return """
            SELECT id, reference_id, matched_reference_id, compatibility_score, group_id, domain_id
            FROM potential_matches
            WHERE group_id = ? AND domain_id = ?
            ORDER BY compatibility_score DESC
            """;
    }

    public static String getPotentialMatchesStreamingSQL() {
        return "SELECT pm FROM PotentialMatchEntity pm WHERE pm.groupId = :groupId AND pm.domainId = :domainId ORDER BY pm.id ASC";
    }

    public static String getNodesTempTableSQL() {
        return  "CREATE TEMP TABLE temp_nodes (\n" +
                "id UUID, \n" +
                "reference_id TEXT, \n" +
                "group_id UUID, \n" +
                "type TEXT, \n" +
                "domain_id UUID, \n" +
                "created_at TIMESTAMP) \n" +
                "ON COMMIT DROP";
    }

    public static String getNodesUpsertSQL() {
        return "INSERT INTO public.nodes (id, reference_id, group_id, type, domain_id, created_at) \n" +
                "SELECT id, reference_id, group_id, type, domain_id, created_at FROM temp_nodes \n" +
                "ON CONFLICT (reference_id, group_id) DO UPDATE SET \n" +
                "type = EXCLUDED.type, domain_id = EXCLUDED.domain_id, created_at = EXCLUDED.created_at \n" +
                "RETURNING id, reference_id, group_id";
    }

    public static String getPrefectMatchesTempTableSQL() {
        return "CREATE TEMP TABLE temp_perfect_matches (\n" +
                "    id UUID NOT NULL,\n" +
                "    group_id UUID,\n" +
                "    domain_id UUID,\n" +
                "    processing_cycle_id VARCHAR(50), \n" +
                "    reference_id VARCHAR(50),\n" +
                "    matched_reference_id VARCHAR(50),\n" +
                "    compatibility_score DOUBLE PRECISION,\n" +
                "    matched_at TIMESTAMP\n" +
                ") ON COMMIT DROP";
    }

    public static String getUpsertPerfectMatchesSql() {
        return "INSERT INTO public.perfect_matches (\n" +
                "    id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at\n" +
                ") SELECT DISTINCT ON (group_id, reference_id, matched_reference_id)\n" +
                "    id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at\n" +
                "FROM temp_perfect_matches\n" +
                "ORDER BY group_id, reference_id, matched_reference_id, compatibility_score DESC, matched_at DESC\n" +
                "ON CONFLICT (group_id, reference_id, matched_reference_id)\n" +
                "DO UPDATE SET\n" +
                "    compatibility_score = EXCLUDED.compatibility_score,\n" +
                "    matched_at = EXCLUDED.matched_at";
    }

    public static String getAllPerfectMatchesStreamingSQL() {
        return """
            SELECT id, reference_id, matched_reference_id, compatibility_score, group_id, domain_id
            FROM perfect_matches
            WHERE group_id = ? AND domain_id = ?
            ORDER BY compatibility_score DESC
            """;
    }
}
