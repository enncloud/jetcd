package com.coreos.jetcd.op;

import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.DeleteOption;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;

/**
 * Etcd Operation
 */
public abstract class Op {

    /**
     * Operation type.
     */
    public enum Type {
        PUT, RANGE, DELETE_RANGE,
    }

    protected final Type       type;
    public final ByteSequence key;

    protected Op(Type type, ByteSequence key) {
        this.type = type;
        this.key = key;
    }

    public static PutOp put(ByteSequence key, ByteSequence value, PutOption option) {
        return new PutOp(key, value, option);
    }

    public static GetOp get(ByteSequence key, GetOption option) {
        return new GetOp(key, option);
    }

    public static DeleteOp delete(ByteSequence key, DeleteOption option) {
        return new DeleteOp(key, option);
    }

    public static final class PutOp extends Op {

        public final ByteSequence value;
        public final PutOption  option;

        protected PutOp(ByteSequence key, ByteSequence value, PutOption option) {
            super(Type.PUT, key);
            this.value = value;
            this.option = option;
        }
    }

    public static final class GetOp extends Op {

        public final GetOption option;

        protected GetOp(ByteSequence key, GetOption option) {
            super(Type.RANGE, key);
            this.option = option;
        }
    }

    public static final class DeleteOp extends Op {

        public final DeleteOption option;

        protected DeleteOp(ByteSequence key, DeleteOption option) {
            super(Type.DELETE_RANGE, key);
            this.option = option;
        }
    }
}