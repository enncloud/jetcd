package com.coreos.jetcd.auth;

import com.coreos.jetcd.data.ByteSequence;

import java.util.Optional;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Permission of role.
 */
public class Perm {

    public static PermBuilder newBuilder(){
        return new PermBuilder();
    }

    public static class PermBuilder{
        private Type type;
        private Optional<ByteSequence> key = Optional.empty();
        private Optional<ByteSequence> endKey = Optional.empty();

        private PermBuilder(){}

        public PermBuilder withType(Type type){
            this.type = type;
            return this;
        }

        public PermBuilder withKey(ByteSequence key){
            this.key = Optional.ofNullable(key);
            return this;
        }

        public PermBuilder withEndKey(ByteSequence endKey){
            this.endKey = Optional.ofNullable(endKey);
            return this;
        }

        public Perm build(){
            checkNotNull(type, "permission type can not be null");
            return new Perm(type, key, endKey);
        }

    }

    public enum Type{
        READ, WRITE, READWRITE
    }
    public final Type type;
    public final Optional<ByteSequence> key;
    public final Optional<ByteSequence> endKey;

    public Perm(Type type, Optional<ByteSequence> key, Optional<ByteSequence> endKey) {
        this.type = type;
        this.key = key;
        this.endKey = endKey;
    }
}
