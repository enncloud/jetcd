package com.coreos.jetcd.auth;

import com.coreos.jetcd.data.ByteSequence;

/**
 * etcd user
 */
public class User {
    public final ByteSequence name;
    public final ByteSequence password;
    public final Role[] roles;

    public User(ByteSequence name, ByteSequence password, Role[] roles) {
        this.name = name;
        this.password = password;
        this.roles = roles;
    }
}
