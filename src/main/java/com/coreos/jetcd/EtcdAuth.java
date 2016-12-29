package com.coreos.jetcd;

import com.coreos.jetcd.auth.Perm;
import com.coreos.jetcd.auth.Role;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * Interface of auth talking to etcd
 */
public interface EtcdAuth {

    // ***************
    // Auth Manage
    // ***************

    CompletableFuture<EtcdHeader> authEnable();

    CompletableFuture<EtcdHeader> authDisable();

    // ***************
    // User Manage
    // ***************

    CompletableFuture<EtcdHeader> userAdd(ByteSequence name, ByteSequence password);

    CompletableFuture<EtcdHeader> userDelete(ByteSequence name);

    CompletableFuture<EtcdHeader> userChangePassword(ByteSequence name, ByteSequence password);

    CompletableFuture<GetUserResult> userGet(ByteSequence name);

    CompletableFuture<ListUserResult> userList();

    // ***************
    // User Role Manage
    // ***************

    CompletableFuture<EtcdHeader> userGrantRole(ByteSequence name, ByteSequence role);

    CompletableFuture<EtcdHeader> userRevokeRole(ByteSequence name, ByteSequence role);

    // ***************
    // Role Manage
    // ***************

    CompletableFuture<EtcdHeader> roleAdd(ByteSequence name);

    CompletableFuture<EtcdHeader> roleGrantPermission(ByteSequence role, Perm perm);

    CompletableFuture<GetRoleResult> roleGet(ByteSequence role);

    CompletableFuture<ListRoleResult> roleList();

    CompletableFuture<EtcdHeader> roleRevokePermission(ByteSequence role, ByteSequence key,
                                                                            ByteSequence rangeEnd);

    CompletableFuture<EtcdHeader> roleDelete(ByteSequence role);

    class GetUserResult{
        public final EtcdHeader header;
        public final ByteSequence user;
        public final String[] roles;

        public GetUserResult(EtcdHeader header, ByteSequence user, String[] roles) {
            this.header = header;
            this.user = user;
            this.roles = Arrays.copyOf(roles, roles.length);
        }
    }

    class ListUserResult{
        public final EtcdHeader header;
        public final String[] users;

        public ListUserResult(EtcdHeader header, String[] users) {
            this.header = header;
            this.users = users;
        }
    }
    
    class GetRoleResult{
        public final EtcdHeader header;
        public final Role role;

        public GetRoleResult(EtcdHeader header, Role role) {
            this.header = header;
            this.role = role;
        }
    }
    
    class ListRoleResult{
        public final EtcdHeader header;
        public final String[] roles;

        public ListRoleResult(EtcdHeader header, String[] roles) {
            this.header = header;
            this.roles = roles;
        }
    }

}
