package com.shedule.x.dto;

public record Candidate(int hash, int score) implements Comparable<Candidate> {
    @Override
    public int compareTo(Candidate o) {
        return Integer.compare(o.score, this.score);
    }
}