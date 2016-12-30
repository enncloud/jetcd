package com.coreos.jetcd;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import com.coreos.jetcd.Cluster.Member;
import com.coreos.jetcd.api.ClusterGrpc;
import com.coreos.jetcd.api.MemberAddRequest;
import com.coreos.jetcd.api.MemberListRequest;
import com.coreos.jetcd.api.MemberRemoveRequest;
import com.coreos.jetcd.api.MemberUpdateRequest;
import com.coreos.jetcd.data.EtcdHeader;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;

/**
 * Implementation of cluster client
 */
public class EtcdClusterImpl implements EtcdCluster {
    private final ClusterGrpc.ClusterFutureStub stub;
    private Supplier<Executor> callExecutor;

    public EtcdClusterImpl(ManagedChannel channel, Optional<String> token){
        this.stub = EtcdClientUtil.configureStub(ClusterGrpc.newFutureStub(channel), token);
        callExecutor = Suppliers.memoize(()-> Executors.newSingleThreadExecutor());
    }

    /**
     * lists the current cluster membership
     *
     * @return
     */
    @Override
    public CompletableFuture<ListMemberResult> listMember() {
        return EtcdUtil.completableFromListenableFuture(stub.memberList(MemberListRequest.getDefaultInstance()), response->{
            Member[] members = new Member[response.getMembersCount()];
            for(int index=0; index<response.getMembersCount(); index++){
                members[index] = convertAPIMember(response.getMembers(index));
            }
            return new ListMemberResult(EtcdUtil.apiToClientHeader(response.getHeader()), members);
        }, callExecutor.get());
    }

    /**
     * add a new member into the cluster
     *
     * @param endpoints the address of the new member
     * @return
     */
    @Override
    public CompletableFuture<AddMemberResult> addMember(List<String> endpoints) {
        MemberAddRequest memberAddRequest = MemberAddRequest.newBuilder().addAllPeerURLs(endpoints).build();
        return EtcdUtil.completableFromListenableFuture(stub.memberAdd(memberAddRequest),
                response->new AddMemberResult(EtcdUtil.apiToClientHeader(response.getHeader()), convertAPIMember(response.getMember())),
                callExecutor.get());
    }

    /**
     * removes an existing member from the cluster
     *
     * @param memberID the id of the member
     * @return
     */
    @Override
    public CompletableFuture<EtcdHeader> removeMember(long memberID) {
        MemberRemoveRequest memberRemoveRequest = MemberRemoveRequest.newBuilder().setID(memberID).build();
        return EtcdUtil.completableFromListenableFuture(stub.memberRemove(memberRemoveRequest),
                response->EtcdUtil.apiToClientHeader(response.getHeader()),
                callExecutor.get());
    }

    /**
     * update peer addresses of the member
     *
     * @param memberID the id of member to update
     * @param endpoints the new endpoints for the member
     * @return
     */
    @Override
    public CompletableFuture<EtcdHeader> updateMember(long memberID, List<String> endpoints) {
        MemberUpdateRequest memberUpdateRequest = MemberUpdateRequest.newBuilder()
                .addAllPeerURLs(endpoints)
                .setID(memberID)
                .build();
        return EtcdUtil.completableFromListenableFuture(stub.memberUpdate(memberUpdateRequest),
                response->EtcdUtil.apiToClientHeader(response.getHeader()),
                callExecutor.get());
    }

    private Member convertAPIMember(com.coreos.jetcd.api.Member apiMember){
        String[] peerURLs = new String[apiMember.getPeerURLsCount()];
        String[] clientURLs = new String[apiMember.getClientURLsCount()];
        return new Member(apiMember.getID(), apiMember.getName(), apiMember.getPeerURLsList().toArray(peerURLs)
                , apiMember.getClientURLsList().toArray(clientURLs));
    }
}
