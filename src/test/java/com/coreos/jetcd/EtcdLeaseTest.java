package com.coreos.jetcd;

import com.coreos.jetcd.api.LeaseKeepAliveResponse;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.lease.Lease;
import com.coreos.jetcd.options.PutOption;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

/**
 * KV service test cases.
 */
public class EtcdLeaseTest {

    private EtcdKV kvClient;
    private EtcdClient etcdClient;
    private EtcdLease leaseClient;
    private Assertion test;


    private ByteSequence testKey = ByteSequence.fromString("foo1");
    private ByteSequence testName = ByteSequence.fromString("bar");

    @BeforeTest
    public void setUp() throws Exception {
        test = new Assertion();
        etcdClient = EtcdClientBuilder.newBuilder().endpoints(TestConstants.endpoints).build();
        kvClient = etcdClient.getKVClient();

        leaseClient = etcdClient.getLeaseClient();
    }

    @Test
    public void testGrant() throws Exception {
        Lease lease = leaseClient.grant(5).get();
        kvClient.put(testKey, testName, PutOption.newBuilder().withLeaseId(lease).build()).get();
        test.assertEquals(kvClient.get(testKey).get().count, 1);

        Thread.sleep(6000);
        test.assertEquals(kvClient.get(testKey).get().count, 0);
    }

    @Test(dependsOnMethods = "testGrant")
    public void testRevoke() throws Exception {
        Lease lease = leaseClient.grant(5).get();
        kvClient.put(testKey, testName, PutOption.newBuilder().withLeaseId(lease).build()).get();
        test.assertEquals(kvClient.get(testKey).get().count, 1);
        leaseClient.revoke(lease).get();
        test.assertEquals(kvClient.get(testKey).get().count, 0);
    }

    @Test(dependsOnMethods = "testRevoke")
    public void testkeepAlive() throws Exception {
        Lease lease = leaseClient.grant(5).get();
        kvClient.put(testKey, testName, PutOption.newBuilder().withLeaseId(lease).build()).get();
        test.assertEquals(kvClient.get(testKey).get().count, 1);
        leaseClient.startKeepAliveService();
        leaseClient.keepAlive(lease, new EtcdLease.EtcdLeaseHandler() {
            @Override
            public void onKeepAliveRespond(LeaseKeepAliveResponse keepAliveResponse) {

            }

            @Override
            public void onLeaseExpired(long leaseId) {

            }

            @Override
            public void onError(Throwable throwable) {

            }
        });
        Thread.sleep(6000);
        test.assertEquals(kvClient.get(testKey).get().count, 1);
        leaseClient.cancelKeepAlive(lease);
        test.assertEquals(kvClient.get(testKey).get().count, 0);
    }
}
