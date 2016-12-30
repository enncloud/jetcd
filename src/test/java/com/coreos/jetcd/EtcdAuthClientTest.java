package com.coreos.jetcd;


import com.coreos.jetcd.api.AuthRoleGetResponse;
import com.coreos.jetcd.api.Permission;
import com.coreos.jetcd.auth.Perm;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.exception.AuthFailedException;
import com.coreos.jetcd.exception.ConnectException;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

import io.grpc.StatusRuntimeException;

/**
 * test etcd auth
 */
public class EtcdAuthClientTest {

    private EtcdAuth authClient;
    private EtcdKV kvClient;

    private ByteSequence roleName = ByteSequence.fromString("root");

    private ByteSequence keyRangeBegin = ByteSequence.fromString("foo");
    private ByteSequence keyRangeEnd = ByteSequence.fromString("zoo");

    private ByteSequence testKey = ByteSequence.fromString("foo1");
    private ByteSequence testName = ByteSequence.fromString("bar");

    private ByteSequence userName = ByteSequence.fromString("root");
    private ByteSequence password = ByteSequence.fromString("123");

    private Assertion test;

    private EtcdClient etcdClient;
    private EtcdClient authEtcdClient;

    /**
     * Build etcd client to create role, permission
     */
    @BeforeTest
    public void setupEnv() throws AuthFailedException, ConnectException {
        this.test = new Assertion();
        this.etcdClient = EtcdClientBuilder.newBuilder().endpoints("localhost:2379").build();
        this.kvClient = this.etcdClient.getKVClient();
        this.authClient = this.etcdClient.getAuthClient();
    }

    /**
     * create role with un-auth etcd client
     */
    @Test(groups = "role")
    public void testRoleAdd() throws ExecutionException, InterruptedException {
        this.authClient.roleAdd(roleName).get();
    }

    /**
     * grant permission to role
     */
    @Test(dependsOnMethods = "testRoleAdd", groups = "role")
    public void testRoleGrantPermission() throws ExecutionException, InterruptedException {
        this.authClient.roleGrantPermission(roleName, Perm.newBuilder().withKey(keyRangeBegin).withEndKey(keyRangeEnd).withType(Perm.Type.READWRITE).build()).get();
    }

    /**
     * add user with password and username
     */
    @Test(groups = "user")
    public void testUserAdd() throws ExecutionException, InterruptedException {
        this.authClient.userAdd(userName, password).get();
    }

    /**
     * grant user role
     */
    @Test(dependsOnMethods = {"testUserAdd", "testRoleGrantPermission"}, groups = "user")
    public void testUserGrantRole() throws ExecutionException, InterruptedException {
        this.authClient.userGrantRole(userName, roleName).get();
    }

    /**
     * enable etcd auth
     */
    @Test(dependsOnGroups = "user", groups = "authEnable")
    public void testEnableAuth() throws ExecutionException, InterruptedException {
        this.authClient.authEnable().get();
    }

    /**
     * auth client with password and user name
     */
    @Test(dependsOnMethods = "testEnableAuth", groups = "authEnable")
    public void setupAuthClient() throws AuthFailedException, ConnectException {
        this.authEtcdClient = EtcdClientBuilder.newBuilder().endpoints("localhost:2379").setName(userName).setPassword(password).build();

    }

    /**
     * put and range with auth client
     */
    @Test(groups = "testAuth", dependsOnGroups = "authEnable")
    public void testKVWithAuth() throws ExecutionException, InterruptedException {
        Throwable err = null;
        try {
            this.authEtcdClient.getKVClient().put(testKey, testName).get();
            EtcdKV.RangeResult rangeResult = this.authEtcdClient.getKVClient().get((testKey)).get();
            this.test.assertTrue(rangeResult.count != 0 && rangeResult.kvs.get(0).getValue().equals(testName));
        } catch (StatusRuntimeException sre) {
            err = sre;
        }
        this.test.assertNull(err, "KV put range test with auth");
    }

    /**
     * put and range with auth client
     */
    @Test(groups = "testAuth", dependsOnGroups = "authEnable")
    public void testKVWithoutAuth() throws InterruptedException {
        Throwable err = null;
        try {
            this.kvClient.put(testKey, testName).get();
            this.kvClient.get(testKey).get();
        } catch (ExecutionException sre) {
            err = sre;
        }
        this.test.assertNotNull(err, "KV put range test without auth");
    }

    /**
     * get auth's permission
     */
    @Test(groups = "testAuth", dependsOnGroups = "authEnable")
    public void testRoleGet() throws ExecutionException, InterruptedException {
        EtcdAuth.GetRoleResult result = this.authEtcdClient.getAuthClient().roleGet(roleName).get();
        this.test.assertTrue(result.role.perms.length != 0);
    }

    /**
     * disable etcd auth
     */
    @Test(dependsOnGroups = "testAuth", groups = "disableAuth")
    public void testDisableAuth() {
        Throwable err = null;
        try {
            this.authEtcdClient.getAuthClient().authDisable().get();
        } catch (Exception e) {
            err = e;
        }
        this.test.assertNull(err, "auth disable");
    }

    /**
     * delete user
     */
    @Test(dependsOnGroups = "disableAuth", groups = "clearEnv")
    public void delUser() {
        Throwable err = null;
        try {
            this.authEtcdClient.getAuthClient().userDelete(userName).get();
        } catch (Exception e) {
            err = e;
        }
        this.test.assertNull(err, "user delete");
    }

    /**
     * delete role
     */
    @Test(dependsOnGroups = "disableAuth", groups = "clearEnv")
    public void delRole() {
        Throwable err = null;
        try {
            this.authEtcdClient.getAuthClient().roleDelete(roleName).get();
        } catch (Exception e) {
            err = e;
        }
        this.test.assertNull(err, "delete role");
    }

}
