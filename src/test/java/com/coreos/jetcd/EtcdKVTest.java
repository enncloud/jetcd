package com.coreos.jetcd;

import com.coreos.jetcd.api.RangeRequest;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.lease.Lease;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.op.Txn;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

import java.util.concurrent.CompletableFuture;

/**
 * KV service test cases.
 */
public class EtcdKVTest {

    private EtcdKV kvClient;
    private Assertion test;

    @BeforeTest
    public void setUp() throws Exception {
        test = new Assertion();
        EtcdClient client = EtcdClientBuilder.newBuilder().endpoints("http://localhost:2379").build();
        kvClient = client.getKVClient();
    }

    @Test
    public void testPut() throws Exception {
        ByteSequence sampleKey = ByteSequence.fromString("sample_key");
        ByteSequence sampleValue = ByteSequence.fromString("sample_value");
        CompletableFuture<EtcdKV.PutResult> feature = kvClient.put(sampleKey, sampleValue);
        try {
            EtcdKV.PutResult result = feature.get();
            test.assertTrue(result.header != null);
            test.assertTrue(result.prevKV == null);
        } catch (Exception e) {
            // empty
            e.printStackTrace();
        }
    }

    @Test
    public void testPutWithNotExistLease() throws Exception {
        ByteSequence sampleKey = ByteSequence.fromString("sample_key");
        ByteSequence sampleValue = ByteSequence.fromString("sample_value");
        PutOption option = PutOption.newBuilder().withLeaseId(new Lease(99999l, 3l, null)).build();
        CompletableFuture<EtcdKV.PutResult> feature = kvClient.put(sampleKey, sampleValue, option);
        try {
            EtcdKV.PutResult result = feature.get();
            test.assertTrue(result.header != null);
        } catch (Exception e) {
            // empty
        }
    }

    @Test
    public void testGet() throws Exception {
        ByteSequence sampleKey = ByteSequence.fromString("sample_key2");
        ByteSequence sampleValue = ByteSequence.fromString("sample_value2");
        CompletableFuture<EtcdKV.PutResult> feature = kvClient.put(sampleKey, sampleValue);
        try {
            feature.get();
            CompletableFuture<EtcdKV.RangeResult> getFeature = kvClient.get(sampleKey);
            EtcdKV.RangeResult result = getFeature.get();
            test.assertEquals(result.kvs.size(), 1);
            test.assertEquals(result.kvs.get(0).getValue().toStringUtf8(), "sample_value2");
            test.assertTrue(!result.more);
        } catch (Exception e) {
            // empty
        }
    }

    @Test
    public void testGetWithRev() throws Exception {
        ByteSequence sampleKey = ByteSequence.fromString("sample_key3");
        ByteSequence sampleValue = ByteSequence.fromString("sample_value");
        ByteSequence sampleValueTwo = ByteSequence.fromString("sample_value2");
        CompletableFuture<EtcdKV.PutResult> feature = kvClient.put(sampleKey, sampleValue);
        try {
            EtcdKV.PutResult putResult = feature.get();
            kvClient.put(sampleKey, sampleValueTwo).get();
            GetOption option = GetOption.newBuilder().withRevision(putResult.header.getRevision()).build();
            CompletableFuture<EtcdKV.RangeResult> getFeature = kvClient.get(sampleKey, option);
            EtcdKV.RangeResult response = getFeature.get();
            test.assertEquals(response.kvs.size(), 1);
            test.assertEquals(response.kvs.get(0).getValue().toStringUtf8(), "sample_value");
        } catch (Exception e) {
            // empty
        }
    }

    @Test
    public void testGetSortedPrefix() throws Exception {
        ByteSequence key = ByteSequence.fromString("test_key");
        ByteSequence testValue = ByteSequence.fromString("test_value");
        for (int i = 0; i < 3; i++) {
            ByteSequence testKey = ByteSequence.fromString("test_key" + i);
            try {
                kvClient.put(testKey, testValue).get();
            } catch (Exception e) {
                // empty
            }
        }
        ByteSequence endKey = ByteSequence.fromString("\0");
        GetOption option = GetOption.newBuilder().withSortField(RangeRequest.SortTarget.KEY).withSortOrder(RangeRequest.SortOrder.DESCEND)
                .withRange(endKey).build();
        try {
            CompletableFuture<EtcdKV.RangeResult> getFeature = kvClient.get(key, option);
            EtcdKV.RangeResult result = getFeature.get();
            test.assertEquals(result.kvs.size(), 3);
            test.assertEquals(result.kvs.get(0).getKey().toStringUtf8(), "test_key2");
            test.assertEquals(result.kvs.get(0).getValue().toStringUtf8(), "test_value");
            test.assertEquals(result.kvs.get(1).getKey().toStringUtf8(), "test_key1");
            test.assertEquals(result.kvs.get(1).getValue().toStringUtf8(), "test_value");
            test.assertEquals(result.kvs.get(2).getKey().toStringUtf8(), "test_key0");
            test.assertEquals(result.kvs.get(2).getValue().toStringUtf8(), "test_value");
        } catch (Exception e) {
            // empty
        }
    }

    @Test(dependsOnMethods = "testPut")
    public void testDelete() throws Exception {
        ByteSequence keyToDelete = ByteSequence.fromString("sample_key");
        try {
            // count keys about to delete
            CompletableFuture<EtcdKV.RangeResult> getFeature = kvClient.get(keyToDelete);
            EtcdKV.RangeResult resp = getFeature.get();

            // delete the keys
            CompletableFuture<EtcdKV.DeleteResult> deleteFuture = kvClient.delete(keyToDelete);
            EtcdKV.DeleteResult deleteResult = deleteFuture.get();
            test.assertEquals(resp.kvs.size(), deleteResult.deleted);
        } catch (Exception e) {
            // empty
        }
    }

    @Test
    public void testTxn() throws Exception {
        ByteSequence sampleKey = ByteSequence.fromString("txn_key");
        ByteSequence sampleValue = ByteSequence.fromString("xyz");
        ByteSequence cmpValue = ByteSequence.fromString("abc");
        ByteSequence putValue = ByteSequence.fromString("XYZ");
        ByteSequence putValueNew = ByteSequence.fromString("ABC");
        CompletableFuture<EtcdKV.PutResult> feature = kvClient.put(sampleKey, sampleValue);
        try {
            // put the original txn key value pair
            feature.get();

            // construct txn operation
            Cmp cmp = new Cmp(sampleKey, Cmp.Op.GREATER, CmpTarget.value(cmpValue));
            Txn txn = Txn.newBuilder().If(cmp).Then(Op.put(sampleKey, putValue, PutOption.DEFAULT))
                    .Else(Op.put(sampleKey, putValueNew, PutOption.DEFAULT)).build();
            CompletableFuture<EtcdKV.TxnResult> txnResp = kvClient.commit(txn);
            txnResp.get();

            // get the value
            EtcdKV.RangeResult rangeResult = kvClient.get(sampleKey).get();
            test.assertEquals(rangeResult.kvs.size(), 1);
            test.assertEquals(rangeResult.kvs.get(0).getValue().toStringUtf8(), "XYZ");
        } catch (Exception e) {
            // empty
        }
        kvClient.delete(sampleKey).get();
    }
}