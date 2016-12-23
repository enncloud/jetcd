package com.coreos.jetcd.op;

import com.coreos.jetcd.data.ByteSequence;

/**
 * The compare predicate in {@link Txn}
 */
public class Cmp {

    public enum Op {
        EQUAL, GREATER, LESS
    }

    public final ByteSequence key;
    public final Op           op;
    public final CmpTarget<?> target;

    public Cmp(ByteSequence key, Op compareOp, CmpTarget<?> target) {
        this.key = key;
        this.op = compareOp;
        this.target = target;
    }
}
