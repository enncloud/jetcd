package com.coreos.jetcd.maintenance;

/**
 * Status of cluster.
 */
public class Status {
    // version is the cluster protocol version used by the responding member.
    public final String version;
    // dbSize is the size of the backend database, in bytes, of the responding member.
    public final long dbSize;
    // leader is the member ID which the responding member believes is the current leader.
    public final long leader;
    // raftIndex is the current raft index of the responding member.
    public final long raftIndex;
    // raftTerm is the current raft term of the responding member.
    public final long raftTerm;

    public Status(String version, long dbSize, long leader, long raftIndex, long raftTerm) {
        this.version = version;
        this.dbSize = dbSize;
        this.leader = leader;
        this.raftIndex = raftIndex;
        this.raftTerm = raftTerm;
    }
}
