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

    /** Verify data integrity using SHA-256 (hex). Mirrors calculate_checksum(data). */
    public static String calculateChecksum(byte[] data) {
        return sha256Hex(data);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /** Current time as Unix epoch seconds (double), matching Python's time.time(). */
    public static double now() {
        return System.currentTimeMillis() / 1000.0;
    }

    /** Convert bytes to a human readable string. Mirrors format_size(). */
    public static String formatSize(double sizeBytes) {
        String[] units = {"B", "KB", "MB", "GB"};
        for (String unit : units) {
            if (sizeBytes < 1024) {
                return String.format("%.1f %s", sizeBytes, unit);
            }
            sizeBytes /= 1024;
        }
        return String.format("%.1f TB", sizeBytes);
    }

    /** Encode bytes as a lowercase hex string (binary-over-JSON, like data.hex()). */
    public static String toHex(byte[] data) {
        return HexFormat.of().formatHex(data);
    }

    /** Decode a hex string back to bytes (bytes.fromhex equivalent). */
    public static byte[] fromHex(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
