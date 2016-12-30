package com.coreos.jetcd;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListenableFuture;

import com.coreos.jetcd.api.AlarmRequest;
import com.coreos.jetcd.api.AlarmResponse;
import com.coreos.jetcd.api.AlarmType;
import com.coreos.jetcd.api.DefragmentRequest;
import com.coreos.jetcd.api.MaintenanceGrpc;
import com.coreos.jetcd.api.SnapshotRequest;
import com.coreos.jetcd.api.SnapshotResponse;
import com.coreos.jetcd.api.StatusRequest;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.maintenance.AlarmAction;
import com.coreos.jetcd.maintenance.AlarmMember;
import com.coreos.jetcd.maintenance.Status;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Implementation of maintenance client
 */
public class EtcdMaintenanceImpl implements EtcdMaintenance {
    private MaintenanceGrpc.MaintenanceFutureStub futureStub;
    private MaintenanceGrpc.MaintenanceStub streamStub;
    private volatile StreamObserver<SnapshotResponse> snapshotObserver;
    private volatile SnapshotCallback snapshotCallback;
    private Supplier<Executor> callExecutor;

    public EtcdMaintenanceImpl(ManagedChannel channel, Optional<String> token) {
        this.futureStub = EtcdClientUtil.configureStub(MaintenanceGrpc.newFutureStub(channel), token);
        this.streamStub = EtcdClientUtil.configureStub(MaintenanceGrpc.newStub(channel), token);
        this.callExecutor = Suppliers.memoize(()-> Executors.newSingleThreadExecutor());
    }

    /**
     * get all active keyspace alarm
     *
     * @return alarm list
     */
    @Override
    public CompletableFuture<ListAlarmsResult> listAlarms() {
        AlarmRequest alarmRequest = AlarmRequest.newBuilder()
                .setAlarm(AlarmType.NONE)
                .setAction(AlarmRequest.AlarmAction.GET)
                .setMemberID(0).build();
        return convertFutureAlarmResponse(this.futureStub.alarm(alarmRequest));
    }

    /**
     * disarms a given alarm
     *
     * @return the response result
     */
    @Override
    public CompletableFuture<ListAlarmsResult> disalarm(long memberID, com.coreos.jetcd.maintenance.AlarmType alarmType, AlarmAction action) {
        AlarmRequest.Builder builder = AlarmRequest.newBuilder()
                .setMemberID(memberID);
        if(alarmType == com.coreos.jetcd.maintenance.AlarmType.NONE){
            builder.setAlarm(AlarmType.NONE);
        }else{
            builder.setAlarm(AlarmType.NOSPACE);
        }

        if(action == AlarmAction.ACTIVATE){
            builder.setAction(AlarmRequest.AlarmAction.ACTIVATE);
        }else if(action == AlarmAction.DEACTIVATE){
            builder.setAction(AlarmRequest.AlarmAction.DEACTIVATE);
        }else{
            builder.setAction(AlarmRequest.AlarmAction.GET);
        }
        checkArgument(memberID != 0, "the member id can not be 0");
        return convertFutureAlarmResponse(this.futureStub.alarm(builder.build()));
    }

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
    @Override
    public CompletableFuture<EtcdHeader> defragmentMember() {
        return EtcdUtil.completableFromListenableFuture(this.futureStub.defragment(DefragmentRequest.getDefaultInstance()),
                response->EtcdUtil.apiToClientHeader(response.getHeader())
                ,callExecutor.get());
    }

    /**
     * get the status of one member
     */
    @Override
    public CompletableFuture<StatusResult> statusMember() {
        return EtcdUtil.completableFromListenableFuture(this.futureStub.status(StatusRequest.getDefaultInstance()),
                response->new StatusResult(EtcdUtil.apiToClientHeader(response.getHeader()),
                        new Status(response.getVersion(), response.getDbSize(), response.getLeader(), response.getRaftIndex(), response.getRaftTerm())),
                callExecutor.get());
    }

    /**
     * Set callback for snapshot
     * <p> The onSnapshot will be called when the member make a snapshot.
     * <p> The onError will be called as exception, and the callback will be canceled.
     *
     * @param callback Snapshot callback
     */
    @Override
    public synchronized void setSnapshotCallback(SnapshotCallback callback) {
        if (this.snapshotObserver == null) {
            this.snapshotObserver = new StreamObserver<SnapshotResponse>() {
                @Override
                public void onNext(SnapshotResponse snapshotResponse) {
                    if (snapshotCallback != null) {
                        synchronized (EtcdMaintenanceImpl.this) {
                            if (snapshotCallback != null) {
                                snapshotCallback.onSnapShot(new SnapshotResult(
                                        EtcdUtil.apiToClientHeader(snapshotResponse.getHeader()),
                                        snapshotResponse.getRemainingBytes(),
                                        EtcdUtil.byteSequceFromByteString(snapshotResponse.getBlob())));
                            }
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    synchronized (EtcdMaintenanceImpl.this) {
                        if (snapshotCallback != null) {
                            snapshotCallback.onError(throwable);
                        }
                        snapshotObserver = null;
                    }
                }

                @Override
                public void onCompleted() {

                }
            };
        }

        this.streamStub.snapshot(SnapshotRequest.getDefaultInstance(), this.snapshotObserver);
    }

    /**
     * Remove callback for snapshot.
     */
    @Override
    public synchronized void removeSnapShotCallback() {
        if(this.snapshotObserver != null){
            snapshotObserver.onCompleted();
            snapshotCallback = null;
            snapshotObserver = null;
        }
    }


    private CompletableFuture<ListAlarmsResult> convertFutureAlarmResponse(ListenableFuture<AlarmResponse> futureResponse){
        return EtcdUtil.completableFromListenableFuture(futureResponse,
                response-> new ListAlarmsResult(
                        EtcdUtil.apiToClientHeader(response.getHeader()),
                        EtcdUtil.convertList(response.getAlarmsList(),
                                alarm->new AlarmMember(alarm.getMemberID(), convertAlarmType(alarm.getAlarm()))).toArray(new AlarmMember[response.getAlarmsCount()]))
        , callExecutor.get());
    }

    private com.coreos.jetcd.maintenance.AlarmType convertAlarmType(AlarmType type){
        switch (type){
            case NONE:
                return com.coreos.jetcd.maintenance.AlarmType.NONE;
            case NOSPACE:
                return com.coreos.jetcd.maintenance.AlarmType.NOSPACE;
        }
        throw new RuntimeException("unrecognized alarm type");
    }
}
