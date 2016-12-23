package com.coreos.jetcd;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;

import com.coreos.jetcd.api.CompactionRequest;
import com.coreos.jetcd.api.Compare;
import com.coreos.jetcd.api.DeleteRangeRequest;
import com.coreos.jetcd.api.DeleteRangeResponse;
import com.coreos.jetcd.api.KVGrpc;
import com.coreos.jetcd.api.PutRequest;
import com.coreos.jetcd.api.PutResponse;
import com.coreos.jetcd.api.RangeRequest;
import com.coreos.jetcd.api.RangeResponse;
import com.coreos.jetcd.api.RequestOp;
import com.coreos.jetcd.api.ResponseOp;
import com.coreos.jetcd.api.TxnRequest;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Op;
import com.coreos.jetcd.op.Txn;
import com.coreos.jetcd.options.CompactOption;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of etcd kv client
 */
class EtcdKVImpl implements EtcdKV {
    private final KVGrpc.KVFutureStub stub;
    private Supplier<Executor> callExecutor;


    EtcdKVImpl(ManagedChannel channel, Optional<String> token) {
        this.stub = EtcdClientUtil.configureStub(KVGrpc.newFutureStub(channel), token);
        callExecutor = Suppliers.memoize(() -> Executors.newSingleThreadExecutor());
    }

    // ***************
    // Op.PUT
    // ***************

    @Override
    public CompletableFuture<PutResult> put(ByteSequence key, ByteSequence value) {
        return put(key, value, PutOption.DEFAULT);
    }

    @Override
    public CompletableFuture<PutResult> put(ByteSequence key, ByteSequence value, PutOption option) {
        checkNotNull(key, "key should not be null");
        checkNotNull(value, "value should not be null");
        checkNotNull(option, "option should not be null");

        PutRequest request = PutRequest.newBuilder()
                .setKey(ByteString.copyFrom(key.getBytes()))
                .setValue(ByteString.copyFrom(value.getBytes()))
                .setLease(option.getLeaseId())
                .setPrevKv(option.getPrevKV())
                .build();

        return EtcdUtil.completableFromListenableFuture(this.stub.put(request), response -> putResponseToResult(response), callExecutor.get());
    }

    // ***************
    // Op.GET
    // ***************

    @Override
    public CompletableFuture<RangeResult> get(ByteSequence key) {
        return get(key, GetOption.DEFAULT);
    }

    @Override
    public CompletableFuture<RangeResult> get(ByteSequence key, GetOption option) {
        checkNotNull(key, "key should not be null");
        checkNotNull(option, "option should not be null");

        RangeRequest.Builder builder = RangeRequest.newBuilder()
                .setKey(EtcdUtil.byteStringFromByteSequence(key))
                .setCountOnly(option.isCountOnly())
                .setLimit(option.getLimit())
                .setRevision(option.getRevision())
                .setKeysOnly(option.isKeysOnly())
                .setSerializable(option.isSerializable())
                .setSortOrder(option.getSortOrder())
                .setSortTarget(option.getSortField());

        if (option.getEndKey().isPresent()) {
            builder.setRangeEnd(EtcdUtil.byteStringFromByteSequence(option.getEndKey().get()));
        }

        return EtcdUtil.completableFromListenableFuture(this.stub.range(builder.build()), (response) -> rangeResponseToResult(response), callExecutor.get());
    }

    // ***************
    // Op.DELETE
    // ***************

    @Override
    public CompletableFuture<DeleteResult> delete(ByteSequence key) {
        return delete(key, DeleteOption.DEFAULT);
    }

    @Override
    public CompletableFuture<DeleteResult> delete(ByteSequence key, DeleteOption option) {
        checkNotNull(key, "key should not be null");
        checkNotNull(option, "option should not be null");

        DeleteRangeRequest.Builder builder = DeleteRangeRequest.newBuilder()
                .setKey(EtcdUtil.byteStringFromByteSequence(key))
                .setPrevKv(option.isPrevKV());

        if (option.getEndKey().isPresent()) {
            builder.setRangeEnd(option.getEndKey().get());
        }
        return EtcdUtil.completableFromListenableFuture(this.stub.deleteRange(builder.build()), (response) -> deleteResponseToResult(response), callExecutor.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> compact() {
        return compact(CompactOption.DEFAULT);
    }

    @Override
    public CompletableFuture<EtcdHeader> compact(CompactOption option) {
        checkNotNull(option, "option should not be null");

        CompactionRequest request = CompactionRequest.newBuilder()
                .setRevision(option.getRevision())
                .setPhysical(option.isPhysical())
                .build();

        return EtcdUtil.completableFromListenableFuture(stub.compact(request), response -> EtcdUtil.apiToClientHeader(response.getHeader(), -1), callExecutor.get());
    }

    @Override
    public CompletableFuture<TxnResult> commit(Txn txn) {
        checkNotNull(txn, "txn should not be null");
        return EtcdUtil.completableFromListenableFuture(this.stub.txn(toTxnRequest(txn)), response -> {
                    List<OperationResult> operationResults = new ArrayList<OperationResult>();
                    for (ResponseOp opResp : response.getResponsesList()) {
                        switch (opResp.getResponseCase()) {
                            case RESPONSE_RANGE:
                                operationResults.add(rangeResponseToResult(opResp.getResponseRange()));
                                break;
                            case RESPONSE_PUT:
                                operationResults.add(putResponseToResult(opResp.getResponsePut()));
                                break;
                            case RESPONSE_DELETE_RANGE:
                                operationResults.add(deleteResponseToResult(opResp.getResponseDeleteRange()));
                                break;
                        }
                    }
                    return new TxnResult(EtcdUtil.apiToClientHeader(response.getHeader(), -1), response.getSucceeded(), operationResults);
                }
                , callExecutor.get());
    }

    private PutResult putResponseToResult(PutResponse response) {
        KeyValue prevKV = null;
        if (response.hasPrevKv()) {
            prevKV = EtcdUtil.apiToClientKV(response.getPrevKv());
        }
        return new PutResult(EtcdUtil.apiToClientHeader(response.getHeader(), -1), prevKV);
    }

    private RangeResult rangeResponseToResult(RangeResponse response) {
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        for (com.coreos.jetcd.api.KeyValue kv : response.getKvsList()) {
            kvs.add(EtcdUtil.apiToClientKV(kv));
        }
        return new RangeResult(EtcdUtil.apiToClientHeader(response.getHeader(), -1), kvs, response.getMore(), response.getCount());
    }

    private DeleteResult deleteResponseToResult(DeleteRangeResponse response) {
        List<KeyValue> prevKVs = new ArrayList<KeyValue>();
        for (com.coreos.jetcd.api.KeyValue kv : response.getPrevKvsList()) {
            prevKVs.add(EtcdUtil.apiToClientKV(kv));
        }
        return new DeleteResult(EtcdUtil.apiToClientHeader(response.getHeader(), -1), response.getDeleted(), prevKVs);
    }

    private Compare toCompare(Cmp cmp) {
        Compare.Builder compareBuiler = Compare.newBuilder().setKey(EtcdUtil.byteStringFromByteSequence(cmp.key));
        switch (cmp.op) {
            case EQUAL:
                compareBuiler.setResult(Compare.CompareResult.EQUAL);
                break;
            case GREATER:
                compareBuiler.setResult(Compare.CompareResult.GREATER);
                break;
            case LESS:
                compareBuiler.setResult(Compare.CompareResult.LESS);
                break;
            default:
                throw new IllegalArgumentException("Unexpected compare type (" + cmp.op + ")");
        }

        Compare.CompareTarget target = cmp.target.getTarget();
        Object value = cmp.target.getTargetValue();

        compareBuiler.setTarget(target);
        switch (target) {
            case VERSION:
                compareBuiler.setVersion((Long) value);
                break;
            case VALUE:
                compareBuiler.setValue(EtcdUtil.byteStringFromByteSequence((ByteSequence) value));
                break;
            case MOD:
                compareBuiler.setModRevision((Long) value);
            case CREATE:
                compareBuiler.setCreateRevision((Long) value);
            default:
                throw new IllegalArgumentException("Unexpected target type (" + target + ")");
        }

        return compareBuiler.build();
    }

    private RequestOp toRequestOp(Op.PutOp op) {
        PutRequest put = PutRequest.newBuilder()
                .setKey(EtcdUtil.byteStringFromByteSequence(op.key))
                .setValue(EtcdUtil.byteStringFromByteSequence(op.value))
                .setLease(op.option.getLeaseId())
                .setPrevKv(op.option.getPrevKV())
                .build();

        return RequestOp.newBuilder().setRequestPut(put).build();
    }

    private RequestOp toRequestOp(Op.GetOp op) {
        RangeRequest.Builder range = RangeRequest.newBuilder()
                .setKey(EtcdUtil.byteStringFromByteSequence(op.key))
                .setCountOnly(op.option.isCountOnly())
                .setLimit(op.option.getLimit())
                .setRevision(op.option.getRevision())
                .setKeysOnly(op.option.isKeysOnly())
                .setSerializable(op.option.isSerializable())
                .setSortOrder(op.option.getSortOrder())
                .setSortTarget(op.option.getSortField());

        if (op.option.getEndKey().isPresent()) {
            range.setRangeEnd(EtcdUtil.byteStringFromByteSequence(op.option.getEndKey().get()));
        }

        return RequestOp.newBuilder().setRequestRange(range).build();
    }

    private RequestOp toRequestOp(Op.DeleteOp op) {
        DeleteRangeRequest.Builder delete = DeleteRangeRequest.newBuilder()
                .setKey(EtcdUtil.byteStringFromByteSequence(op.key))
                .setPrevKv(op.option.isPrevKV());

        if (op.option.getEndKey().isPresent()) {
            delete.setRangeEnd(op.option.getEndKey().get());
        }

        return RequestOp.newBuilder().setRequestDeleteRange(delete).build();
    }

    private RequestOp toRequestOp(Op op) {
        if (op instanceof Op.PutOp) {
            return toRequestOp((Op.PutOp) op);
        }
        if (op instanceof Op.DeleteOp) {
            return toRequestOp((Op.DeleteOp) op);
        }
        if (op instanceof Op.GetOp) {
            return toRequestOp((Op.GetOp) op);
        }
        throw new RuntimeException("unknown operation");
    }

    private TxnRequest toTxnRequest(Txn txn) {
        TxnRequest.Builder requestBuilder = TxnRequest.newBuilder();

        for (Cmp c : txn.cmpList) {
            requestBuilder.addCompare(toCompare(c));
        }

        for (Op o : txn.successOpList) {
            requestBuilder.addSuccess(toRequestOp(o));
        }

        for (Op o : txn.failureOpList) {
            requestBuilder.addFailure(toRequestOp(o));
        }

        return requestBuilder.build();
    }
}
