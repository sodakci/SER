package graph;

public enum EdgeType {
    WW, RW, WR, SO,
    /** Predicate-read read dependency: flip-witness writer → reader (graph A / readFrom role). */
    PR_WR,
    /** Predicate-read anti dependency: reader → post-read flip-witness writer (graph B role). */
    PR_RW
}
