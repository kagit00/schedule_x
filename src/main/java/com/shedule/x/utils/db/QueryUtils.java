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
                    "WHERE rn <= 200\n" +
                    "ON CONFLICT (group_id, reference_id, matched_reference_id)\n" +
                    "DO UPDATE SET\n" +
                    "    compatibility_score = EXCLUDED.compatibility_score,\n" +
                    "    matched_at = EXCLUDED.matched_at";

    private static final String DROP_TEMP_TABLE_SQL =
            "DROP TABLE IF EXISTS temp_potential_matches";

    private static final String TEMP_TABLE_SQL =
            "CREATE TEMPORARY TABLE IF NOT EXISTS temp_potential_matches (\n" +
                    "    id UUID NOT NULL,\n" +
                    "    group_id VARCHAR(50),\n" +
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

    public static String getDropTempTableSql() {
        return DROP_TEMP_TABLE_SQL;
    }

    public static String getTempTableSql() {
        return TEMP_TABLE_SQL;
    }
}
