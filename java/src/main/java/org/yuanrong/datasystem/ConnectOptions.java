/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yuanrong.datasystem;

import java.util.Arrays;
import java.util.Objects;

/**
 * The connection options.
 *
 * @since 2022-08-05
 */
public class ConnectOptions {
    /**
     * The Worker's IPv4 address.
     */
    private String host;

    /**
     * The port of the worker service.
     */
    private int port;

    /**
     * Timeout for establishing a connection with a worker, and the default value is
     * 60s.
     */
    private int connectTimeoutMs = 60 * 1000;

    /**
     * Specify a tenant's token for tenant authentication.
     */
    private byte[] token;

    /**
     *
     */
    private int requestTimeoutMs = 0;

    /**
     * The client private key.
     */
    private String clientPublicKey = "";

    /**
     * The client private key.
     */
    private byte[] clientPrivateKey;

    /**
     * The worker private key.
     */
    private String serverPublicKey = "";

    /**
     * The access key for AK/SK authorize.
     */
    private String accessKey = "";

    /**
     * The secret key for AK/SK authorize.
     */
    private byte[] secretKey;

    /**
     * The tenant ID.
     */
    private String tenantID = "";

    /**
     * Indicates whether the client can connect to the standby node.
     */
    private boolean enableCrossNodeConnection = false;

    /**
     * Empty constructors for ConnectOptions.
     */
    public ConnectOptions() {
        this.host = "";
        this.port = 0;
    }

    /**
     * ConnectOptions Structure Method.
     *
     * @param host The Worker's IPv4 address.
     * @param port The port of the worker service.
     */
    public ConnectOptions(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void setConnectTimeout(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public void setRequestTimeout(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public void setEnableCrossNodeConnection(boolean enableCrossNodeConnection) {
        this.enableCrossNodeConnection = enableCrossNodeConnection;
    }

    public void setToken(byte[] token) {
        this.token = token == null ? null : token.clone();
    }

    public void setClientPublicKey(String clientPublicKey) {
        this.clientPublicKey = clientPublicKey;
    }

    public void setClientPrivateKey(byte[] clientPrivateKey) {
        this.clientPrivateKey = clientPrivateKey == null ? null : clientPrivateKey.clone();
    }

    public void setServerPublicKey(String serverPublicKey) {
        this.serverPublicKey = serverPublicKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(byte[] secretKey) {
        this.secretKey = secretKey == null ? null : secretKey.clone();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public void setAkSkAuth(String accessKey, byte[] secretKey, String tenantID) {
        this.accessKey = accessKey;
        this.secretKey = secretKey == null ? null : secretKey.clone();
        this.tenantID = tenantID;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ConnectOptions otherConnectOptions = (ConnectOptions) other;
        return port == otherConnectOptions.port
                && connectTimeoutMs == otherConnectOptions.connectTimeoutMs
                && requestTimeoutMs == otherConnectOptions.requestTimeoutMs
                && enableCrossNodeConnection == otherConnectOptions.enableCrossNodeConnection
                && host.equals(otherConnectOptions.host)
                && Arrays.equals(token, otherConnectOptions.token)
                && clientPublicKey.equals(otherConnectOptions.clientPublicKey)
                && Arrays.equals(clientPrivateKey, otherConnectOptions.clientPrivateKey)
                && serverPublicKey.equals(otherConnectOptions.serverPublicKey)
                && accessKey.equals(otherConnectOptions.accessKey)
                && Arrays.equals(secretKey, otherConnectOptions.secretKey)
                && tenantID.equals(otherConnectOptions.tenantID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, token, clientPublicKey, clientPrivateKey, serverPublicKey, accessKey, secretKey,
                            tenantID, enableCrossNodeConnection);
    }
}