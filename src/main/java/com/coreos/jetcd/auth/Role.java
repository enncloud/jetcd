package com.coreos.jetcd.auth;

import com.coreos.jetcd.data.ByteSequence;

/**
 * Role authed to user.
 */
public class Role {

    public final ByteSequence roleName;
    public final Perm[] perms;

    public Role(ByteSequence roleName, Perm[] perms) {
        this.roleName = roleName;
        this.perms = perms;
    }
}
