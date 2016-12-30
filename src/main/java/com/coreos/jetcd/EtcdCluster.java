package com.coreos.jetcd;

import com.coreos.jetcd.Cluster.Member;
import com.coreos.jetcd.data.EtcdHeader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface of cluster client talking to etcd
 */
public interface EtcdCluster {

    /**
     * lists the current cluster membership
     *
     * @return
     */
    CompletableFuture<ListMemberResult> listMember();

    /**
     * add a new member into the cluster
     *
     * @param endpoints the address of the new member
     * @return
     */
    CompletableFuture<AddMemberResult> addMember(List<String> endpoints);

    /**
     * removes an existing member from the cluster
     *
     * @param memberID
     * @return
     */
    CompletableFuture<EtcdHeader> removeMember(long memberID);

    /**
     * update peer addresses of the member
     *
     * @param memberID
     * @param endpoints
     * @return
     */
    CompletableFuture<EtcdHeader> updateMember(long memberID, List<String> endpoints);

    class ListMemberResult{
        public final EtcdHeader header;
        public final Member[] members;

        public ListMemberResult(EtcdHeader header, Member[] members) {
            this.header = header;
            this.members = members;
        }
    }

    class AddMemberResult{
        public final EtcdHeader header;
        public final Member member;

        public AddMemberResult(EtcdHeader header, Member member) {
            this.header = header;
            this.member = member;
        }
    }
}
