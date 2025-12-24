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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/**
 * To load related files
 *
 * @since 2022-05-07
 */
public class Utils {
    private static final Logger log = Logger.getLogger(Utils.class.getName());
    private static final Set<String> loaded = new HashSet<String>();
    private static final int READ_SIZE = 8192; // 8KB
    private static String jniDir = System.getenv("DATASYSTEM_JNI_PATH");
    private static String loadDsClientStr = System.getenv("LOAD_DS_CLIENT_IN_JVM");     // default is true
    private static String[] keys = { // the libraries are sorted by dependency order
            "libtbb",
            "libcurl",
            "libds-spdlog",
            "libzmq",
            "libsecurec",
            "libdatasystem",
            "libclient_jni_api"
    };
    private static String mayNoNeedLibs = "libzmq";

    // The so libraries which contain global variables will cause redifinition error in reloading scenario.
    // To avoid this, the libraries in reloadDsClientNoNeedLibs will not be loaded twice.
    private static String reloadDsClientNoNeedLibs = "libds-spdlog|libdatasystem";
    private static String newLine = "[\r\n]";

    /**
     * load the dependent library
     */
    public static void loadLibrary() {
        boolean shouldLoadDsClient = true;
        if (loadDsClientStr != null) {
            shouldLoadDsClient = Boolean.parseBoolean(loadDsClientStr);
        }
        log.info(() -> "the env for DATASYSTEM_JNI_PATH:" + jniDir + ", LOAD_DS_CLIENT_IN_JVM:" + loadDsClientStr);
        String libRoot = File.separator + "native" + File.separator + getArchitecture();
        String propPath = libRoot + File.separator + "so.properties";
        try (InputStream in = Utils.class.getResourceAsStream(propPath)) {
            if (in == null) {
                throw new ExceptionInInitializerError(propPath + " not exists.");
            }
            Properties props = new Properties();
            props.load(in);
            for (String key : keys) {
                String name = props.getProperty(key);
                String hash = props.getProperty(key + ".hash");
                if (name == null || hash == null) {
                    if (mayNoNeedLibs.contains(key)) {
                        log.info(() -> "load " + key + " failed, may be loaded statically");
                        continue;
                    }
                    throw new InvalidPropertiesFormatException("the path or hash is empty for " + key);
                }
                if (!shouldLoadDsClient && reloadDsClientNoNeedLibs.contains(key)) {
                    log.info(() -> "skip to load " + key);
                    continue;
                }
                runLoad(libRoot + File.separator + name, hash);
                log.info(() -> "success to load " + key);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * load the so file from jar.
     *
     * @param path The so path.
     * @param hash The SHA-256 for this file.
     */
    private static synchronized void runLoad(String path, String hash) {
        String name = new File(path).getName();
        if (loaded.contains(name)) {
            log.info(() -> "already loaded " + path.replaceAll(newLine, ""));
            return;
        }

        boolean isSuccess = false;
        try {
            String jniExtraPath = File.separator + ".datasystem" + File.separator + "jni";
            if (jniDir == null || jniDir.isEmpty()) {
                jniDir = Paths.get(System.getProperty("user.home"), ".datasystem", "jni").toString();
            } else if (!jniDir.contains(jniExtraPath)) {
                jniDir = jniDir + jniExtraPath;
            } else {
                jniDir = jniDir;
            }
            // Check whether the so file exists.
            // If it exists, check whether sha-256 is consistent. Otherwise, executes the
            // logic of loading the persistent SO file.
            // If sha-256 is consistent, return directly. Otherwise, execute the logic for
            // loading the temporary SO file.
            File persSoFile = Paths.get(jniDir, name).toFile();
            isSuccess = persSoFile.exists() ? (load(persSoFile, hash) || loadTemporarySoFile(path, hash))
                    : loadPersistentSoFile(path, hash);
        } catch (SecurityException | IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        if (!isSuccess) {
            throw new ExceptionInInitializerError("unable to load " + path);
        } else {
            loaded.add(name);
        }
    }

    /**
     * Write the so file in the JAR package to the local so file.
     *
     * @param path       The path fo so file.
     * @param outChannel The output file channel.
     * @return true if and only the so file is successfully copied;false otherwise.
     * @throws IOException If an I/O error occurs.
     */
    private static boolean copyJarSoToLocal(String path, FileChannel outChannel) throws IOException {
        try (InputStream in = Utils.class.getResourceAsStream(path)) {
            if (in == null) {
                log.warning(() -> "skipping for " + path.replaceAll(newLine, "") + " not found");
                return false;
            }
            outChannel.transferFrom(Channels.newChannel(in), 0, Long.MAX_VALUE);
            return true;
        }
    }

    /**
     * Write the so file in the JAR file to the local persistent so file and load
     * the file to the memory.
     *
     * @param path The path fo so file.
     * @param hash The SHA-256 for this file.
     * @return true if and only the so file is successfully loaded;false otherwise.
     * @throws IOException If an I/O error occurs.
     */
    private static boolean loadPersistentSoFile(String path, String hash) throws IOException {
        File dir = Paths.get(jniDir).toFile();
        if (!dir.exists() && !dir.mkdirs()) {
            log.warning(() -> "create dir failed :" + jniDir);
        }

        String soName = new File(path).getName();
        File persSoFile = Paths.get(jniDir, soName).toFile();
        if (!persSoFile.createNewFile() && !persSoFile.exists()) {
            log.warning(() -> "create so file " + soName + " failed");
            return false;
        }
        // Attempt to obtain the file lock, non-blocking waiting.
        // The process that obtains the lock executes the logic of loading the
        // persistent SO file. Otherwise, the logic of loading the temporary SO file is
        // executed.
        boolean isLockFailed;
        try (FileChannel outChannel = FileChannel.open(persSoFile.toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
                FileLock soLock = outChannel.lock()) {
            isLockFailed = soLock == null;
            // Only the first process can copy file to local filesystem.
            if (!isLockFailed && outChannel.size() == 0 && !copyJarSoToLocal(path, outChannel)) {
                return false;
            }
        }

        return isLockFailed ? loadTemporarySoFile(path, hash) : load(persSoFile, hash);
    }

    /**
     * Write the so file in the JAR file to the local temporary so file, load the
     * file to the memory, and automatically delete the file when the program ends.
     *
     * @param path The path fo so file.
     * @param hash The SHA-256 for this file.
     * @return true if and only the so file is successfully loaded;false otherwise.
     * @throws IOException If an I/O error occurs.
     */
    private static boolean loadTemporarySoFile(String path, String hash) throws IOException {
        String soName = new File(path).getName();
        File tempSoFile = File.createTempFile("tmp", soName, Paths.get(jniDir).toFile());
        tempSoFile.deleteOnExit();
        try (FileChannel outChannel = FileChannel.open(tempSoFile.toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            return copyJarSoToLocal(path, outChannel) && load(tempSoFile, hash);
        }
    }

    /**
     * Load the so file from the local host to the memory.
     *
     * @param file The so file object.
     * @param hash The SHA-256 for this file.
     * @return true if and only the so file is successfully loaded;false otherwise.
     */
    private static boolean load(File file, String hash) {
        try {
            if (MessageDigest.isEqual(hash.getBytes(StandardCharsets.UTF_8),
                    getSHA256(file).getBytes(StandardCharsets.UTF_8))) {
                System.load(file.getCanonicalPath());
                return true;
            }
        } catch (UnsatisfiedLinkError | NoSuchAlgorithmException | IOException e) {
            if (!file.delete()) {
                log.warning(() -> "failed to delete" + file.getName() + ":" + e.getMessage());
            }
            throw new ExceptionInInitializerError(e);
        }
        return false;
    }

    /**
     * Compute the SHA-256 message digest.
     *
     * @param file The file object.
     * @return The SHA-256 message digest.
     * @throws NoSuchAlgorithmException If not support SHA-256 algorithm.
     * @throws IOException              If an I/O error occurs.
     */
    private static String getSHA256(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream is = Files.newInputStream(file.toPath())) {
            byte[] buf = new byte[READ_SIZE];
            int len = is.read(buf);
            while (len > 0) {
                md.update(buf, 0, len);
                len = is.read(buf);
            }
        }

        StringBuilder sb = new StringBuilder();
        for (byte oneByte : md.digest()) {
            sb.append(String.format("%02x", oneByte));
        }
        return sb.toString();
    }

    /**
     * Get the cpu architecture, only support aarch64 and x86_64.
     *
     * @return The cpu architecture.
     */
    private static String getArchitecture() {
        String architecture = null;
        String name = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ENGLISH);
        if (name.contains("nix") || name.contains("nux")) {
            if (arch.contains("aarch64")) {
                architecture = "aarch64";
            } else if ((arch.contains("86") || arch.contains("amd")) && arch.contains("64")) {
                architecture = "x86_64";
            } else {
                architecture = null;
            }
        }

        if (architecture == null) {
            throw new ExceptionInInitializerError(
                    "Not support in current system, os.name is " + name + ", os.arch is " + arch);
        }
        return architecture;
    }
}