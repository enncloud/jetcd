package com.coreos.jetcd;

import com.coreos.jetcd.exception.AuthFailedException;
import com.coreos.jetcd.exception.ConnectException;

import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Maintenance test.
 */
public class MaintenanceTest {

    private final EtcdClient etcdClient;
    private final EtcdMaintenance maintenance;
    private final Assertion test = new Assertion();
    private volatile byte[] snapshotBlob;
    private CountDownLatch finishLatch = new CountDownLatch(1);

    public MaintenanceTest() throws AuthFailedException, ConnectException {
        this.etcdClient = EtcdClientBuilder.newBuilder().endpoints(TestConstants.endpoints).build();
        maintenance = etcdClient.getMaintenanceClient();
    }

    /**
     * test status member function
     */
    @Test
    public void testStatusMember() throws ExecutionException, InterruptedException {
        EtcdMaintenance.StatusResult statusResult = maintenance.statusMember().get();
        test.assertTrue(statusResult.status.dbSize > 0);
    }

    /**
     * test alarm list function
     * TODO trigger alarm, valid whether listAlarms will work.
     * TODO disarm the alarm member, valid whether disarm will work with listAlarms.
     */
    @Test
    public void testAlarmList() throws ExecutionException, InterruptedException {
        maintenance.listAlarms().get();
    }

    /**
     * test setSnapshotCallback function
     * TODO trigger snapshot, valid whether setSnapshotCallback will work.
     * TODO test removeSnapShotCallback
     */
    @Test
    void testAddSnapshotCallback() {
        maintenance.setSnapshotCallback(new EtcdMaintenance.SnapshotCallback() {
            @Override
            public synchronized void onSnapShot(EtcdMaintenance.SnapshotResult snapshotResult) {
                // blob contains the next chunk of the snapshot in the snapshot stream, blob is the bytes snapshot.
                // remaining_bytes is the number of blob bytes to be sent after this message
                byte[] newBlock = snapshotResult.blob.getBytes();
                if (snapshotBlob == null) {
                    snapshotBlob = newBlock;
                } else {
                    byte[] temp = new byte[newBlock.length+snapshotBlob.length];
                    System.arraycopy(snapshotBlob, 0, temp, 0, snapshotBlob.length);
                    System.arraycopy(newBlock, 0, temp, snapshotBlob.length, newBlock.length);
                }
                if (snapshotResult.remaining_bytes == 0) {
                    // TODO finishLatch will be replaced by ListenableFuture instance
                    finishLatch.countDown();
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }
        });
    }

    /**
     * test defragmentMember function
     */
    @Test
    void testDefragment() throws ExecutionException, InterruptedException {
        maintenance.defragmentMember().get();
    }
}
