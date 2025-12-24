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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Java cluster.
 *
 * @since 2022-11-28
 */
public class Cluster {
    private static final Logger log = Logger.getLogger(Cluster.class.getName());
    private static final int WAIT_TIMEOUT_SECS = 15;
    private static String newLine = "[\r\n]";

    private String fileSeparator = File.separator;
    private int vLogLevel = 0;
    private String workerGflagParams = "";

    private String rootDir = resolvePath(System.getProperty("llt.dir"));
    private String socketDir = fileSeparator + "tmp" + fileSeparator + "socket_"
            + new SecureRandom().nextInt(Integer.MAX_VALUE);

    private List<Process> workerProcesses = new ArrayList<Process>();
    private List<Process> redisProcesses = new ArrayList<Process>();
    private List<Process> etcdsProcesses = new ArrayList<Process>();

    private List<String> workerIpAddrs = new ArrayList<String>();
    private List<String> redisIpAddrs = new ArrayList<String>();
    private List<List<String>> etcdIpAddrs = new ArrayList<List<String>>();

    private List<String> healthCheckFileList = new ArrayList<String>();

    private int numWorkers;
    private int numRedis;
    private int numEtcds;

    public Cluster() {
        this(1, 0, 1);
    }

    public Cluster(int numWorkers, int numRedis) {
        this(numWorkers, numRedis, 1);
    }

    public Cluster(int numWorkers, int numRedis, int numEtcds) {
        this.numWorkers = numWorkers;
        this.numRedis = numRedis;
        this.numEtcds = numEtcds;
    }

    enum ClusterNodeType {
        ALL, MASTER, GCS, WORKER, ETCD, REDIS
    }

    /**
     * Set the worker parameters.
     *
     * @param workerGflagParams The worker parameters.
     */
    public void setWorkerGflagParams(String workerGflagParams) {
        this.workerGflagParams = workerGflagParams;
    }

    /**
     * Init the cluster.
     *
     * @param name The test group name.
     * @throws IOException If an I/O error occurs.
     */
    public void init(String name) throws IOException {
        rootDir = rootDir + fileSeparator + "JavaApiTest." + name;
        File dir = Paths.get(rootDir).toFile();
        if (dir.exists()) {
            try {
                deleteDirectory(dir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!dir.mkdirs()) {
            log.warning("create dir failed.");
        }

        if (!Paths.get(socketDir).toFile().mkdirs()) {
            log.warning("create socket dir failed.");
        }

        String localhost = "127.0.0.1";
        for (int i = 0; i < numWorkers; i++) {
            workerIpAddrs.add(localhost + ":" + getFreePort());
        }

        for (int i = 0; i < numRedis; i++) {
            redisIpAddrs.add(localhost + ":" + getFreePort());
        }

        for (int i = 0; i < numEtcds; i++) {
            List<String> etcdAddr = new ArrayList<String>();
            etcdAddr.add(localhost + ":" + getFreePort());
            etcdAddr.add(localhost + ":" + getFreePort());
            etcdIpAddrs.add(etcdAddr);
        }
    }

    /**
     * Start the worker.
     */
    public void startAll() {
        for (int i = 0; i < numEtcds; i++) {
            startEtcds(i);
        }

        for (int i = 0; i < numRedis; i++) {
            startRedis(i);
        }

        for (int i = 0; i < numWorkers; i++) {
            startWorker(i, workerIpAddrs.get(0));
        }
    }

    /**
     * Stop the worker.
     */
    public void stopAll() {
        // If the master exits too quickly, the worker may not receive the first
        // heartbeat from the master and waits.
        try {
            Thread.sleep(500L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Process process : redisProcesses) {
            process.destroy();
        }
        redisProcesses.clear();

        for (Process process : workerProcesses) {
            process.destroyForcibly();
        }
        workerProcesses.clear();

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Process process : etcdsProcesses) {
            process.destroy();
        }
        etcdsProcesses.clear();

        File dir = Paths.get(socketDir).toFile();
        if (dir.exists()) {
            try {
                deleteDirectory(dir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        deleteHealthCheckFile();
    }

    /**
     * Get the worker listen address.
     *
     * @param index The worker index.
     * @return The worker listen address.
     */
    public String getWorkerAddr(int index) {
        return workerIpAddrs.get(index);
    }

    /**
     * Start worker
     *
     * @param index      The worker index.
     * @param masterAddr The master address.
     */
    private void startWorker(int index, String masterAddr) {
        String workerCmd = resolvePath(System.getProperty("worker.bin"));
        String workDir = this.rootDir + fileSeparator + "worker" + index;
        String healthFile = workDir + fileSeparator + "health";
        workerCmd += " -master_address=" + workerIpAddrs.get(0) + " -worker_address=" + workerIpAddrs.get(index)
                + " -rocksdb_store_dir=" + workDir + fileSeparator + "rocksdb" + " -unix_domain_socket_dir="
                + socketDir + " -log_dir=" + workDir + fileSeparator + "log" + " -v=" + vLogLevel
                + " --enable_distributed_master=false"
                + " -health_check_path=" + healthFile + " " + workerGflagParams;
        if (etcdIpAddrs.size() > 0) {
            StringBuilder etcdUrl = new StringBuilder();
            for (int i = 0; i < numEtcds; ++i) {
                if (etcdUrl.length() > 0) {
                    etcdUrl.append(',');
                }
                etcdUrl.append(etcdIpAddrs.get(i).get(0));
            }
            workerCmd += " -etcd_address=" + etcdUrl;
        }
        if (numRedis > 0) {
            // current only support one redis server
            workerCmd += " -l2_cache_type=redis -redis_address=" + redisIpAddrs.get(0);
        }
        String message = "Launch worker [" + index + "] command: " + workerCmd;
        log.info(message.replaceAll(newLine, ""));
        Process process = startNode(workerCmd);
        workerProcesses.add(process);
        waitNodeReady(ClusterNodeType.WORKER, index, WAIT_TIMEOUT_SECS);
        log.info(() -> "Launch worker [" + index + "] success");
    }

    /**
     * Start etcds
     *
     * @param index The etcd index.
     */
    private void startEtcds(int index) {
        String etcdsDir = rootDir + File.separator + "etcdclusters/etcd" + index;
        File dir = new File(etcdsDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                log.info(() -> "Failed to create the folder, which is : " + dir.toString().replaceAll(newLine, ""));
            }
        }
        String clientUrl = "http://" + etcdIpAddrs.get(index).get(0);
        String serverUrl = "http://" + etcdIpAddrs.get(index).get(1);
        StringBuilder clusterUrl = new StringBuilder();
        for (int i = 0; i < etcdIpAddrs.size(); i++) {
            if (clusterUrl.length() > 0) {
                clusterUrl.append(',');
            }
            clusterUrl.append("etcd").append(i).append("=http://").append(etcdIpAddrs.get(i).get(1));
        }
        String etcdCmd = searchPath("etcd") + " -name etcd" + index + " --data-dir " + etcdsDir
                + " --log-level info" + " --listen-client-urls " + clientUrl + " --advertise-client-urls "
                + clientUrl + " --listen-peer-urls " + serverUrl + " --initial-advertise-peer-urls "
                + serverUrl + " --initial-cluster " + clusterUrl + " --initial-cluster-token etcd-cluster";
        String message = "Launch etcd [" + index + "] command: " + etcdCmd;
        log.info(message.replaceAll(newLine, ""));
        Process process = startNode(etcdCmd);
        etcdsProcesses.add(process);
        waitNodeReady(ClusterNodeType.ETCD, index, WAIT_TIMEOUT_SECS);
        log.info(() -> "Launch etcd [" + index + "] success");
    }

    /**
     * Start redis
     *
     * @param index The redis index.
     */
    private void startRedis(int index) {
        String redisDir = rootDir + File.separator + "redis" + File.separator;
        File dir = new File(redisDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                log.info(() -> "Failed to create the folder, which is : " + dir.toString().replaceAll(newLine, ""));
            }
        }
        String ipAddr = redisIpAddrs.get(index);
        String port = ipAddr.split(":")[1];
        String redisCmd = "redis-server --port " + port + " --dir " + redisDir;
        String message = "Launch redis [" + index + "] command: " + redisCmd;
        log.info(message.replaceAll(newLine, ""));
        Process process = startNode(redisCmd);
        redisProcesses.add(process);
        waitNodeReady(ClusterNodeType.REDIS, index, WAIT_TIMEOUT_SECS);
        log.info(() -> "Launch redis [" + index + "] success");
    }

    /**
     * Start cluster node and inherit input and output stream.
     *
     * @param cmdStr The start command.
     * @return The process object.
     */
    private Process startNode(String cmdStr) {
        Process process = null;
        String[] cmd = {"/bin/bash", "-c", cmdStr};
        try {
            process = new ProcessBuilder(cmd).inheritIO().start();
        } catch (IOException e) {
            String message = "execute [" + cmdStr + "] failed!";
            log.warning(message.replaceAll(newLine, ""));
            e.printStackTrace();
        }

        if (process == null) {
            throw new DataSystemException("Process is null after execute [" + cmdStr + "]");
        } else {
            return process;
        }
    }

    /**
     * Get a free port.
     *
     * @return The free port.
     * @throws IOException If an I/O error occurs.
     */
    private int getFreePort() throws IOException {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(0);
            return serverSocket.getLocalPort();
        } catch (IOException exception) {
            log.info("Failed to get free port.");
            return -1;
        } finally {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }

    /**
     * Search exec path.
     *
     * @param exec The exec file name.
     * @return The exec path.
     */
    private String searchPath(String exec) {
        String paths = System.getenv("PATH");
        String fullPath;
        String[] pathArray = paths.split(":");
        for (String path : pathArray) {
            fullPath = path + File.separator + exec;
            File file = new File(fullPath);
            if (file.exists()) {
                return fullPath;
            }
        }
        return "";
    }

    /**
     * Get the real path, even the file or dir not exists.
     *
     * @param path The path.
     * @return The resolved path.
     */
    private static String resolvePath(String path) {
        try {
            return Paths.get(path).toFile().getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
            return path;
        }
    }

    /**
     * Waiting for Node Startup
     *
     * @param nodeType    Service Node Type
     * @param index       Node index.
     * @param timeoutSecs Waiting time. If the node is not ready after the  timeoutSecs time, an exception is thrown.
     */
    private void waitNodeReady(ClusterNodeType nodeType, int index, int timeoutSecs) {
        String healthCheckPath = "";
        switch (nodeType) {
            case WORKER:
                healthCheckPath = this.rootDir + fileSeparator + "worker" + index + fileSeparator + "health";
                healthCheckFileList.add(healthCheckPath);
                checkHealthFile(healthCheckPath, timeoutSecs);
                break;
            case ETCD:
                try {
                    waitEtcdReadyOrTimeout(timeoutSecs);
                } catch (IOException e) {
                    throw new DataSystemException(5, e.getMessage(), e.getCause());
                }
                break;
            case REDIS:
                try {
                    waitRedisReadyOrTimeout(timeoutSecs);
                } catch (IOException e) {
                    throw new DataSystemException(5, e.getMessage(), e.getCause());
                }
                break;
            default:
                throw new DataSystemException(2, "Invalid nodeType:" + nodeType);
        }
    }

    /**
     * BufferedReader's helper, make executing multiple commands look the same as executing one.
     */
    static class BufferedReaderHelper implements Closeable {
        private List<BufferedReader> bufferedReaders = new ArrayList<BufferedReader>();

        /**
         * Add bufferedReader.
         *
         * @param src The inputStream to be added.
         */
        public void add(InputStream src) {
            InputStreamReader inputStream = new InputStreamReader(src, StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(inputStream);
            bufferedReaders.add(bufferedReader);
        }

        /**
         * Read bufferedReader by line.
         *
         * @param keyWord The key word to determine whether the component is ok.
         * @return The success number.
         * @throws IOException If an I/O error occurs.
         */
        public int readLine(String keyWord) throws IOException {
            int successNum = 0;
            for (BufferedReader bufferedReader : bufferedReaders) {
                String line = "";
                while ((line = bufferedReader.readLine()) != null) {
                    if (line.contains(keyWord)) {
                        successNum++;
                    }
                }
            }
            return successNum;
        }

        /**
         * Clear bufferedReaders.
         *
         * @throws IOException If an I/O error occurs.
         */
        public void clear() throws IOException {
            close();
            bufferedReaders.clear();
        }

        /**
         * Close bufferedReaders.
         *
         * @throws IOException If an I/O error occurs.
         */
        public void close() throws IOException {
            IOException exception = null;
            for (BufferedReader bufferedReader : bufferedReaders) {
                try {
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }
                } catch (IOException e) {
                    exception = e;
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
    }

    /**
     * Waiting for component Startup
     *
     * @param timeoutSecs Waiting time. If the node is not ready after the timeoutSecs time, an exception is thrown.
     * @param componentName The component name.
     * @param keyWord The key word to determine whether the component is ok.
     * @param cmds The commands to be execute.
     * @throws IOException If an I/O error occurs.
     */
    private void waitComponentReadyOrTimeout(int timeoutSecs, String componentName, String keyWord,
                                             Collection<String> cmds) throws IOException {
        long startTime = System.currentTimeMillis();
        long deadLine = startTime + timeoutSecs * 1000L;
        try (BufferedReaderHelper bufferedReaderHelper = new BufferedReaderHelper()) {
            while (true) {
                bufferedReaderHelper.clear();
                for (String cmd : cmds) {
                    Process process = Runtime.getRuntime().exec(cmd);
                    bufferedReaderHelper.add(process.getInputStream());
                    bufferedReaderHelper.add(process.getErrorStream());
                }
                int successNum = bufferedReaderHelper.readLine(keyWord);
                if (successNum == cmds.size()) {
                    break;
                } else {
                    long currTime = System.currentTimeMillis();
                    if (currTime > deadLine) {
                        throw new DataSystemException(1002, String.format(Locale.ROOT,
                                "Process startup" + componentName + "cluster timed out and takes %d.", timeoutSecs));
                    }
                    int intervalMs = 10;
                    try {
                        Thread.sleep(intervalMs);  // to check whether cluster is health, intervalMs: 10ms.
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Waiting for Etcd cluster Startup
     *
     * @param timeoutSecs Waiting time. If the node is not ready after the timeoutSecs time, an exception is thrown.
     * @throws IOException If an I/O error occurs.
     */
    private void waitEtcdReadyOrTimeout(int timeoutSecs) throws IOException {
        List<String> cmds = new ArrayList<String>();
        String clusterUrls = "";
        for (List<String> etcdIpAddr : etcdIpAddrs) {
            if (clusterUrls.length() != 0) {
                clusterUrls = clusterUrls.concat(",");
            }
            clusterUrls = clusterUrls.concat(etcdIpAddr.get(0));
        }
        String cmd = "etcdctl --endpoints " + clusterUrls + " endpoint health";
        cmds.add(cmd);
        waitComponentReadyOrTimeout(timeoutSecs, "etcd", "is healthy", cmds);
    }

    /**
     * Waiting for Redis cluster Startup
     *
     * @param timeoutSecs Waiting time. If the node is not ready after the timeoutSecs time, an exception is thrown.
     * @throws IOException Throws IO exception.
     */
    private void waitRedisReadyOrTimeout(int timeoutSecs) throws IOException {
        List<String> cmds = new ArrayList<String>(redisIpAddrs.size());
        for (String redisIpAddr : redisIpAddrs) {
            String delimiter = ":";
            String[] ipPort = redisIpAddr.split(delimiter);
            cmds.add("redis-cli -h " + ipPort[0] + " -p " + ipPort[1] + " ping");
        }
        waitComponentReadyOrTimeout(timeoutSecs, "redis", "PONG", cmds);
    }

    /**
     * Waiting for Worker cluster Startup.
     *
     * @param filepath Path of the health file.
     * @param timeoutSecs Waiting time. If the node is not ready after the timeoutSecs time, an exception is thrown.
     */
    private void checkHealthFile(String filepath, int timeoutSecs) {
        long startTime = System.currentTimeMillis();
        long deadLine = startTime + timeoutSecs * 1000L;
        while (true) {
            File file = new File(filepath);
            if (file.exists()) {
                return;
            } else {
                long currTime = System.currentTimeMillis();
                if (currTime > deadLine) {
                    throw new DataSystemException(1002, "checkHealthFile timed out," + filepath);
                }
                int tmpIntervalMs = 10;
                try {
                    Thread.sleep(tmpIntervalMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Delete the health file after the test is complete.
     */
    private void deleteHealthCheckFile() {
        for (String healthFile : healthCheckFileList) {
            File file = new File(healthFile);
            if (file.exists()) {
                if (!file.delete()) {
                    log.warning(() -> "fail to delete the health file:" + healthFile);
                }
            }
        }
    }

    /**
     * Delete the directory.
     *
     * @param dir The File object for directory.
     * @throws IOException Throws IO exception.
     */
    private static void deleteDirectory(File dir) throws IOException {
        if (dir == null || !dir.isDirectory() || !dir.exists()) {
            return;
        }

        Path path = dir.toPath();
        if (Files.isSymbolicLink(path)) {
            if (!dir.delete()) {
                log.warning(() -> "Failed to delete directory : " + path.toString().replaceAll(newLine, ""));
            }
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException exception) throws IOException {
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        Cluster otherCluster = (Cluster) other;
        return numWorkers == otherCluster.numWorkers
                && numRedis == otherCluster.numRedis
                && workerGflagParams.equals(otherCluster.workerGflagParams)
                && rootDir.equals(otherCluster.rootDir)
                && socketDir.equals(otherCluster.socketDir)
                && workerProcesses.equals(otherCluster.workerProcesses)
                && redisProcesses.equals(otherCluster.redisProcesses)
                && workerIpAddrs.equals(otherCluster.workerIpAddrs)
                && redisIpAddrs.equals(otherCluster.redisIpAddrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerIpAddrs, redisIpAddrs);
    }
}
