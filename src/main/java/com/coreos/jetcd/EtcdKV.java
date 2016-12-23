package com.coreos.jetcd;

import com.google.common.annotations.Beta;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.op.Txn;
import com.coreos.jetcd.options.CompactOption;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface of kv client talking to etcd
 */
@Beta
public interface EtcdKV {

    // ***************
    // Op.PUT
    // ***************

    CompletableFuture<PutResult> put(ByteSequence key, ByteSequence value);

    CompletableFuture<PutResult> put(ByteSequence key, ByteSequence value, PutOption option);

    // ***************
    // Op.GET
    // ***************

    CompletableFuture<RangeResult> get(ByteSequence key);

    CompletableFuture<RangeResult> get(ByteSequence key, GetOption option);

    // ***************
    // Op.DELETE
    // ***************

    CompletableFuture<DeleteResult> delete(ByteSequence key);

    CompletableFuture<DeleteResult> delete(ByteSequence key, DeleteOption option);

    // ***************
    // Op.COMPACT
    // ***************

    CompletableFuture<EtcdHeader> compact();

    CompletableFuture<EtcdHeader> compact(CompactOption option);

    /**
     * Commit a transaction built from {@link com.coreos.jetcd.op.Txn.Builder}.
     *
     * @param txn txn to commit
     */
    CompletableFuture<TxnResult> commit(Txn txn);


    class OperationResult {

    }

    /**
     * Result returned by Put Operation
     */
    class PutResult extends OperationResult {

        public final EtcdHeader header;
        public final KeyValue prevKV;

        public PutResult(EtcdHeader header, KeyValue prevKV) {
            this.header = header;
            this.prevKV = prevKV;
        }
    }

    /**
     * Result returned by Range Operation
     */
    class RangeResult extends OperationResult {

        public final EtcdHeader header;

        // kvs is the list of key-value pairs matched by the range request.
        // kvs is empty when count is requested.
        public final List<KeyValue> kvs;

        // more indicates if there are more keys to return in the requested range.
        public final boolean more;
        // count is set to the number of keys within the range when requested.
        public final long count;

        public RangeResult(EtcdHeader header, List<KeyValue> kvs, boolean more, long count) {
            this.header = header;
            this.kvs = kvs;
            this.more = more;
            this.count = count;
        }
    }

    class DeleteResult extends OperationResult {

        public final EtcdHeader header;

        // deleted is the number of keys deleted by the delete range request.
        public final long deleted;
        // if prev_kv is set in the request, the previous key-value pairs will be returned.
        public final List<KeyValue> prev_kvs;

        public DeleteResult(EtcdHeader header, long deleted, List<KeyValue> prev_kvs) {
            this.header = header;
            this.deleted = deleted;
            this.prev_kvs = prev_kvs;
        }
    }

    class TxnResult {

        public final EtcdHeader header;

        public final boolean succeeded;

        public final List<OperationResult>  results;

        public TxnResult(EtcdHeader header, boolean succeeded, List<OperationResult> results) {
            this.header = header;
            this.succeeded = succeeded;
            this.results = results;
        }
    }
}
