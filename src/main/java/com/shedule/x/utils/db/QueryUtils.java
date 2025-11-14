package com.shedule.x.utils.db;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class QueryUtils {

    private static final String ACQUIRE_GROUP_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(? )::int8)";
    private static final String POTENTIAL_MATCHES_TEMP_TABLE_SQL =
            """
            CREATE TEMP TABLE IF NOT EXISTS temp_potential_matches (
                id UUID NOT NULL,
                group_id UUID,
                domain_id UUID,
                processing_cycle_id VARCHAR(50),
                reference_id VARCHAR(50),
                matched_reference_id VARCHAR(50),
                compatibility_score DOUBLE PRECISION,
                matched_at TIMESTAMP
            ) ON COMMIT DROP
            """;
    private static final String MERGE_POTENTIAL_MATCHES_SQL = """
    /* ---- 2.1 delete obsolete ---- */
    WITH del AS (
        DELETE FROM public.potential_matches pm
        USING   temp_potential_matches tmp
        WHERE   pm.group_id = ?
          AND   pm.group_id = tmp.group_id
          AND   pm.reference_id = tmp.reference_id
          AND   pm.matched_reference_id = tmp.matched_reference_id
          AND   pm.processing_cycle_id <> tmp.processing_cycle_id
    )
    /* ---- 2.2 upsert new/updated (top-500 per reference_id) ---- */
    INSERT INTO public.potential_matches (
       id, group_id, domain_id, processing_cycle_id,
       reference_id, matched_reference_id,
       compatibility_score, matched_at
    )
    SELECT DISTINCT ON (match_id)
           id, group_id, domain_id, processing_cycle_id,
           reference_id, matched_reference_id,
           compatibility_score, matched_at
    FROM (
           SELECT *,
                  LEAST(reference_id, matched_reference_id)
                  || '_' ||
                  GREATEST(reference_id, matched_reference_id) AS match_id,
                  ROW_NUMBER() OVER (
                      PARTITION BY group_id, reference_id
                      ORDER BY compatibility_score DESC
                  ) AS rn
           FROM   temp_potential_matches
           WHERE  group_id = ?
             AND  processing_cycle_id = ?
         ) ranked
    WHERE rn <= 500
    ON CONFLICT (group_id, reference_id, matched_reference_id)
    DO UPDATE SET
         compatibility_score = EXCLUDED.compatibility_score,
         matched_at         = EXCLUDED.matched_at;
    """;

    public static String getPotentialMatchesTempTableSql() {
        return POTENTIAL_MATCHES_TEMP_TABLE_SQL;
    }

    public static String getMergePotentialMatchesSql() {
        return MERGE_POTENTIAL_MATCHES_SQL;
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

    public static String getAcquireGroupLockSql() {
        return ACQUIRE_GROUP_LOCK_SQL;
    }
}
