package com.coreos.jetcd.Cluster;

/**
 * Cluster member
 */
public class Member {
    //ID is the member ID for this member
    public final long id;
    //name is the human-readable name of the member. If the member is not started, the name will be an empty string.
    public final String name;
    //peerURLs is the list of URLs the member exposes to the cluster for communication.
    public final String[] peerURLs;
    //clientURLs is the list of URLs the member exposes to clients for communication. If the member is not started, clientURLs will be empty.
    public final String[] clientURLs;

    public Member(long id, String name, String[] peerURLs, String[] clientURLs) {
        this.id = id;
        this.name = name;
        this.peerURLs = peerURLs;
        this.clientURLs = clientURLs;
    }
}
