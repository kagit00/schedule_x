package com.shedule.x.dto.enums;

public enum NodeType {
    // --- Bipartite domain roles ---
    TASK,
    AGENT,

    // --- Symmetric relationship roles ---
    USER,
    MENTOR,
    MENTEE,
    FRIEND,
    SEEKER,
    PROVIDER,

    MATCHMAKER,
    COUPLE_CANDIDATE,

    ADMIN,
    BOT,
    UNKNOWN
}
