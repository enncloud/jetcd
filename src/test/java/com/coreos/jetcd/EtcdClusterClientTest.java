package com.coreos.jetcd;

import com.coreos.jetcd.Cluster.Member;
import com.coreos.jetcd.exception.AuthFailedException;
import com.coreos.jetcd.exception.ConnectException;

import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * test etcd cluster client
 */
public class EtcdClusterClientTest {

    private Assertion assertion = new Assertion();
    private Member addedMember;

    /**
     * test list cluster function
     */
    @Test
    public void testListCluster() throws ExecutionException, InterruptedException, AuthFailedException, ConnectException {
        EtcdClient etcdClient = EtcdClientBuilder.newBuilder().endpoints(TestConstants.endpoints).build();
        EtcdCluster clusterClient = etcdClient.getClusterClient();
        EtcdCluster.ListMemberResult result = clusterClient.listMember().get();
        assertion.assertEquals(result.members.length, 3, "Members: " + result.members.length);
    }

    /**
     * test add cluster function, added member will be removed by testDeleteMember
     */
    @Test(dependsOnMethods = "testListCluster")
    public void testAddMember() throws AuthFailedException, ConnectException, ExecutionException, InterruptedException, TimeoutException {
        EtcdClient etcdClient = EtcdClientBuilder.newBuilder().endpoints(Arrays.copyOfRange(TestConstants.endpoints, 0, 2)).build();
        EtcdCluster clusterClient = etcdClient.getClusterClient();
        EtcdCluster.ListMemberResult result = clusterClient.listMember().get();
        assertion.assertEquals(result.members.length, 3);
        CompletableFuture<EtcdCluster.AddMemberResult> responseListenableFuture = clusterClient.addMember(Arrays.asList(Arrays.copyOfRange(TestConstants.peerUrls, 2, 3)));
        EtcdCluster.AddMemberResult addMemberResult = responseListenableFuture.get(5, TimeUnit.SECONDS);
        addedMember = addMemberResult.member;
        assertion.assertNotNull(addedMember, "added member: " + addedMember.id);
    }

    /**
     * test update peer url for member
     */
    @Test(dependsOnMethods = "testAddMember")
    public void testUpdateMember() {

        Throwable throwable = null;
        try {
            EtcdClient etcdClient = EtcdClientBuilder.newBuilder().endpoints(Arrays.copyOfRange(TestConstants.endpoints, 1, 3)).build();
            EtcdCluster clusterClient = etcdClient.getClusterClient();
            EtcdCluster.ListMemberResult result = clusterClient.listMember().get();
            String[] newPeerUrl = new String[]{"http://localhost:12380"};
            clusterClient.updateMember(result.members[0].id, Arrays.asList(newPeerUrl)).get();
            clusterClient.updateMember(result.members[0].id, Arrays.asList(result.members[0].peerURLs)).get();
        } catch (Exception e) {
            System.out.println(e);
            throwable = e;
        }
        assertion.assertNull(throwable, "update for member");
    }

    /**
     * test remove member from cluster, the member is added by testAddMember
     */
    @Test(dependsOnMethods = "testUpdateMember")
    public void testDeleteMember() throws ExecutionException, InterruptedException, AuthFailedException, ConnectException {
        EtcdClient etcdClient = EtcdClientBuilder.newBuilder().endpoints(Arrays.copyOfRange(TestConstants.endpoints, 0, 2)).build();
        EtcdCluster clusterClient = etcdClient.getClusterClient();
        clusterClient.removeMember(addedMember.id).get();
        int newCount = clusterClient.listMember().get().members.length;
        assertion.assertEquals(newCount, 3, "delete added member(" + addedMember.id +"), and left " + newCount + " members");
    }


}
