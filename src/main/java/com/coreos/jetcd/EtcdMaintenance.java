package com.coreos.jetcd;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.maintenance.AlarmAction;
import com.coreos.jetcd.maintenance.AlarmMember;
import com.coreos.jetcd.maintenance.Status;

import java.util.concurrent.CompletableFuture;

/**
 * Interface of maintenance talking to etcd
 * <p>
 * An etcd cluster needs periodic maintenance to remain reliable. Depending
 * on an etcd application's needs, this maintenance can usually be
 * automated and performed without downtime or significantly degraded
 * performance.
 * <p>
 * All etcd maintenance manages storage resources consumed by the etcd
 * keyspace. Failure to adequately control the keyspace size is guarded by
 * storage space quotas; if an etcd member runs low on space, a quota will
 * trigger cluster-wide alarms which will put the system into a
 * limited-operation maintenance mode. To avoid running out of space for
 * writes to the keyspace, the etcd keyspace history must be compacted.
 * Storage space itself may be reclaimed by defragmenting etcd members.
 * Finally, periodic snapshot backups of etcd member state makes it possible
 * to recover any unintended logical data loss or corruption caused by
 * operational error.
 */
public interface EtcdMaintenance {

    /**
     * get all active keyspace alarm
     */
    CompletableFuture<ListAlarmsResult> listAlarms();

    /**
     * disarms a given alarm
     *
     * @return the response result
     */
    CompletableFuture<ListAlarmsResult> disalarm(long memberID, com.coreos.jetcd.maintenance.AlarmType alarmType, AlarmAction action);

    /**
     * defragment one member of the cluster
     * <p>
     * After compacting the keyspace, the backend database may exhibit internal
     * fragmentation. Any internal fragmentation is space that is free to use
     * by the backend but still consumes storage space. The process of
     * defragmentation releases this storage space back to the file system.
     * Defragmentation is issued on a per-member so that cluster-wide latency
     * spikes may be avoided.
     * <p>
     * Defragment is an expensive operation. User should avoid defragmenting
     * multiple members at the same time.
     * To defragment multiple members in the cluster, user need to call defragment
     * multiple times with different endpoints.
     */
    CompletableFuture<EtcdHeader> defragmentMember();

    /**
     * get the status of one member
     */
    CompletableFuture<StatusResult> statusMember();

    /**
     * Set callback for snapshot.
     * <p> The onSnapshot will be called when the member make a snapshot.
     * <p> The onError will be called as exception, and the callback will be canceled.
     *
     * @param callback Snapshot callback
     */
    void setSnapshotCallback(SnapshotCallback callback);

    /**
     * Remove callback for snapshot.
     */
    void removeSnapShotCallback();

    /**
     * Callback to process snapshot events.
     */
    interface SnapshotCallback{

        /**
         * The onSnapshot will be called when the member make a snapshot
         *
         * @param snapshotResponse snapshot response
         */
        void onSnapShot(SnapshotResult snapshotResponse);

        /**
         * The onError will be called as exception, and the callback will be canceled.
         *
         * @param throwable
         */
        void onError(Throwable throwable);
    }

    class ListAlarmsResult{
        public final EtcdHeader header;
        public final AlarmMember[] alarms;

        public ListAlarmsResult(EtcdHeader header, AlarmMember[] alarms) {
            this.header = header;
            this.alarms = alarms;
        }
    }

    class StatusResult{
        public final EtcdHeader header;
        public final Status status;

        public StatusResult(EtcdHeader header, Status status) {
            this.header = header;
            this.status = status;
        }
    }

    class SnapshotResult{
        // header has the current key-value store information. The first header in the snapshot
        // stream indicates the point in time of the snapshot.
        public final EtcdHeader header;

        // remaining_bytes is the number of blob bytes to be sent after this message
        public final long remaining_bytes;

        // blob contains the next chunk of the snapshot in the snapshot stream.
        public final ByteSequence blob;

        public SnapshotResult(EtcdHeader header, long remaining_bytes, ByteSequence blob) {
            this.header = header;
            this.remaining_bytes = remaining_bytes;
            this.blob = blob;
        }
    }
}
