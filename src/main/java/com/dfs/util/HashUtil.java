package com.dfs.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Hashing / ID / formatting helpers. Port of shared/utils.py.
 */
public final class HashUtil {

    private HashUtil() {
    }

    /**
     * Create a unique block ID for a file part using SHA-256.
     * Mirrors generate_block_id(filename, part_number): sha256("{filename}_{part}_{uuid8}").
     */
    public static String generateBlockId(String filename, int partNumber) {
        String uuid8 = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        String unique = filename + "_" + partNumber + "_" + uuid8;
        return sha256Hex(unique.getBytes(StandardCharsets.UTF_8));
    }


}
