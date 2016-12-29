package com.coreos.jetcd;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import com.coreos.jetcd.api.AuthDisableRequest;
import com.coreos.jetcd.api.AuthEnableRequest;
import com.coreos.jetcd.api.AuthGrpc;
import com.coreos.jetcd.api.AuthRoleAddRequest;
import com.coreos.jetcd.api.AuthRoleDeleteRequest;
import com.coreos.jetcd.api.AuthRoleGetRequest;
import com.coreos.jetcd.api.AuthRoleGrantPermissionRequest;
import com.coreos.jetcd.api.AuthRoleListRequest;
import com.coreos.jetcd.api.AuthRoleRevokePermissionRequest;
import com.coreos.jetcd.api.AuthUserAddRequest;
import com.coreos.jetcd.api.AuthUserChangePasswordRequest;
import com.coreos.jetcd.api.AuthUserDeleteRequest;
import com.coreos.jetcd.api.AuthUserGetRequest;
import com.coreos.jetcd.api.AuthUserGrantRoleRequest;
import com.coreos.jetcd.api.AuthUserListRequest;
import com.coreos.jetcd.api.AuthUserRevokeRoleRequest;
import com.coreos.jetcd.api.Permission;
import com.coreos.jetcd.auth.Perm;
import com.coreos.jetcd.auth.Role;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.EtcdHeader;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;

/**
 * Implementation of etcd auth client
 */
public class EtcdAuthImpl implements EtcdAuth {
    
    private final AuthGrpc.AuthFutureStub stub;
    private Supplier<Executor> callExector;
    
    public EtcdAuthImpl(ManagedChannel channel, Optional<String> token) {
        this.stub = EtcdClientUtil.configureStub(AuthGrpc.newFutureStub(channel), token);
        callExector = Suppliers.memoize(()-> Executors.newSingleThreadExecutor());
    }

    // ***************
    // Auth Manage
    // ***************

    @Override
    public CompletableFuture<EtcdHeader> authEnable() {
        AuthEnableRequest enableRequest = AuthEnableRequest.getDefaultInstance();
        return EtcdUtil.completableFromListenableFuture(this.stub.authEnable(enableRequest), request->EtcdUtil.apiToClientHeader(request.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> authDisable() {
        AuthDisableRequest disableRequest = AuthDisableRequest.getDefaultInstance();
        return EtcdUtil.completableFromListenableFuture(this.stub.authDisable(disableRequest), request->EtcdUtil.apiToClientHeader(request.getHeader()), callExector.get());
    }

    // ***************
    // User Manage
    // ***************

    @Override
    public CompletableFuture<EtcdHeader> userAdd(ByteSequence name, ByteSequence password) {
        AuthUserAddRequest addRequest = AuthUserAddRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name))
                .setPasswordBytes(EtcdUtil.byteStringFromByteSequence(password))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.userAdd(addRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> userDelete(ByteSequence name) {
        AuthUserDeleteRequest deleteRequest = AuthUserDeleteRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name)).build();
        return EtcdUtil.completableFromListenableFuture(this.stub.userDelete(deleteRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> userChangePassword(ByteSequence name, ByteSequence password) {
        AuthUserChangePasswordRequest changePasswordRequest = AuthUserChangePasswordRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name))
                .setPasswordBytes(EtcdUtil.byteStringFromByteSequence(password))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.userChangePassword(changePasswordRequest),response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<GetUserResult> userGet(ByteSequence name) {
        AuthUserGetRequest userGetRequest = AuthUserGetRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name))
                .build();

        return EtcdUtil.completableFromListenableFuture(this.stub.userGet(userGetRequest), response->{
            String[] roles = new String[response.getRolesCount()];

            for(int index=0; index<response.getRolesCount(); index++){
                roles[index] = response.getRoles(index);
            }
            return new GetUserResult(EtcdUtil.apiToClientHeader(response.getHeader()), name, roles);
        }, callExector.get());
    }

    @Override
    public CompletableFuture<ListUserResult> userList() {
        AuthUserListRequest userListRequest = AuthUserListRequest.getDefaultInstance();
        return EtcdUtil.completableFromListenableFuture(this.stub.userList(userListRequest), response->{
            String[] users = new String[response.getUsersCount()];
            return new ListUserResult(EtcdUtil.apiToClientHeader(response.getHeader()), users);
        }, callExector.get());
    }

    // ***************
    // User Role Manage
    // ***************

    @Override
    public CompletableFuture<EtcdHeader> userGrantRole(ByteSequence name, ByteSequence role) {
        AuthUserGrantRoleRequest userGrantRoleRequest = AuthUserGrantRoleRequest.newBuilder()
                .setUserBytes(EtcdUtil.byteStringFromByteSequence(name))
                .setRoleBytes(EtcdUtil.byteStringFromByteSequence(role))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.userGrantRole(userGrantRoleRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> userRevokeRole(ByteSequence name, ByteSequence role) {
        AuthUserRevokeRoleRequest userRevokeRoleRequest = AuthUserRevokeRoleRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name))
                .setRoleBytes(EtcdUtil.byteStringFromByteSequence(role))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.userRevokeRole(userRevokeRoleRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    // ***************
    // Role Manage
    // ***************

    @Override
    public CompletableFuture<EtcdHeader> roleAdd(ByteSequence name) {
        AuthRoleAddRequest roleAddRequest = AuthRoleAddRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(name))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleAdd(roleAddRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> roleGrantPermission(ByteSequence role, Perm p) {
        AuthRoleGrantPermissionRequest roleGrantPermissionRequest = AuthRoleGrantPermissionRequest.newBuilder()
                .setNameBytes(EtcdUtil.byteStringFromByteSequence(role))
                .setPerm(convertToAPIPerm(p))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleGrantPermission(roleGrantPermissionRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()),callExector.get());
    }

    @Override
    public CompletableFuture<GetRoleResult> roleGet(ByteSequence role) {
        AuthRoleGetRequest roleGetRequest = AuthRoleGetRequest.newBuilder()
                .setRoleBytes(EtcdUtil.byteStringFromByteSequence(role))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleGet(roleGetRequest), response->{
            Perm[] perms = new Perm[response.getPermCount()];
            for(int index=0; index<response.getPermCount(); index++){
                perms[index] = convertFromAPIPerm(response.getPerm(index));
            }
            return new GetRoleResult(EtcdUtil.apiToClientHeader(response.getHeader()), new Role(role, perms));
        }, callExector.get());
    }

    @Override
    public CompletableFuture<ListRoleResult> roleList() {
        AuthRoleListRequest roleListRequest = AuthRoleListRequest.getDefaultInstance();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleList(roleListRequest), response->{
            String[] roles = new String[response.getRolesCount()];
            for(int index = 0; index< response.getRolesCount(); index++){
                roles[index] = response.getRoles(index);
            }
            return new ListRoleResult(EtcdUtil.apiToClientHeader(response.getHeader()), roles);
        }, callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> roleRevokePermission(ByteSequence role, ByteSequence key, ByteSequence rangeEnd) {

        AuthRoleRevokePermissionRequest roleRevokePermissionRequest = AuthRoleRevokePermissionRequest.newBuilder()
                .setRoleBytes(EtcdUtil.byteStringFromByteSequence(role))
                .setKeyBytes(EtcdUtil.byteStringFromByteSequence(key))
                .setRangeEndBytes(EtcdUtil.byteStringFromByteSequence(rangeEnd))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleRevokePermission(roleRevokePermissionRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    @Override
    public CompletableFuture<EtcdHeader> roleDelete(ByteSequence role) {

        AuthRoleDeleteRequest roleDeleteRequest = AuthRoleDeleteRequest.newBuilder()
                .setRoleBytes(EtcdUtil.byteStringFromByteSequence(role))
                .build();
        return EtcdUtil.completableFromListenableFuture(this.stub.roleDelete(roleDeleteRequest), response->EtcdUtil.apiToClientHeader(response.getHeader()), callExector.get());
    }

    private Permission convertToAPIPerm(Perm p){
        Permission.Builder permBuilder = Permission.newBuilder();
        if(p.key.isPresent())permBuilder.setKey(EtcdUtil.byteStringFromByteSequence(p.key.get()));
        if(p.endKey.isPresent())permBuilder.setRangeEnd(EtcdUtil.byteStringFromByteSequence(p.endKey.get()));
        switch (p.type){
            case READ:
                permBuilder.setPermType(Permission.Type.READ);
                break;
            case WRITE:
                permBuilder.setPermType(Permission.Type.WRITE);
                break;
            case READWRITE:
                permBuilder.setPermType(Permission.Type.READWRITE);
                break;
        }
        return permBuilder.build();
    }

    private Perm convertFromAPIPerm(Permission p){
        Perm.PermBuilder builder = Perm.newBuilder();
        if(!p.getKey().isEmpty())
        {
            builder.withKey(EtcdUtil.byteSequceFromByteString(p.getKey()));
        }
        if(!p.getRangeEnd().isEmpty()){
            builder.withEndKey(EtcdUtil.byteSequceFromByteString(p.getRangeEnd()));
        }
        switch (p.getPermType()){
            case READ:
                builder.withType(Perm.Type.READ);
                break;
            case WRITE:
                builder.withType(Perm.Type.WRITE);
                break;
            case READWRITE:
                builder.withType(Perm.Type.READWRITE);
                break;
        }
        return builder.build();
    }

}
