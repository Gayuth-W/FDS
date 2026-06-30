package com.dfs.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Thin wrapper over the JDK HttpClient for node-to-node RPC.
 *
 * Chosen over WebClient/RestClient because the original implementation relies
 * heavily on tight per-request timeouts (httpx timeout=0.2 / 0.5 / 2.0). The
 * JDK client supports per-request timeouts directly and runs cleanly on
 * virtual threads.
 *
 * All RPC bodies are JSON. Binary blocks are hex-encoded inside JSON, exactly
 * like the original /replicate contract.
 */
@Component
public class RpcClient {

    private static final Logger log = LoggerFactory.getLogger(RpcClient.class);

    private final HttpClient client;
    private final ObjectMapper mapper;

    public RpcClient(ObjectMapper mapper) {
        this.mapper = mapper;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    /** POST a JSON body and return the parsed JSON response (empty on any failure). */
    public Optional<JsonNode> postJson(String url, Object body, Duration timeout) {
        try {
            byte[] payload = mapper.writeValueAsBytes(body);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                    .build();
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 200) {
                return Optional.of(mapper.readTree(resp.body()));
            }
            return Optional.empty();
        } catch (Exception e) {
            log.debug("postJson to {} failed: {}", url, e.toString());
            return Optional.empty();
        }
    }

    /** POST a JSON body, returning only whether the call returned HTTP 200. */
    public boolean postOk(String url, Object body, Duration timeout) {
        try {
            byte[] payload = mapper.writeValueAsBytes(body);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                    .build();
            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.debug("postOk to {} failed: {}", url, e.toString());
            return false;
        }
    }

    /** GET and parse JSON. */
    public Optional<JsonNode> getJson(String url, Duration timeout) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .GET()
                    .build();
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 200) {
                return Optional.of(mapper.readTree(resp.body()));
            }
            return Optional.empty();
        } catch (Exception e) {
            log.debug("getJson from {} failed: {}", url, e.toString());
            return Optional.empty();
        }
    }

    /** GET raw bytes (used to pull file content from a peer during recovery). */
    public Optional<byte[]> getBytes(String url, Duration timeout) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .GET()
                    .build();
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 200) {
                return Optional.of(resp.body());
            }
            return Optional.empty();
        } catch (Exception e) {
            log.debug("getBytes from {} failed: {}", url, e.toString());
            return Optional.empty();
        }
    }

    /**
     * POST a multipart/form-data file (single "file" part). Used during recovery
     * to re-upload a recovered file to a peer's /files endpoint.
     */
    public boolean postMultipartFile(String url, String filename, byte[] content, Duration timeout) {
        try {
            String boundary = "----DFSBoundary" + System.nanoTime();
            String preamble = "--" + boundary + "\r\n"
                    + "Content-Disposition: form-data; name=\"file\"; filename=\"" + filename + "\"\r\n"
                    + "Content-Type: application/octet-stream\r\n\r\n";
            String epilogue = "\r\n--" + boundary + "--\r\n";

            var out = new java.io.ByteArrayOutputStream();
            out.write(preamble.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            out.write(content);
            out.write(epilogue.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(out.toByteArray()))
                    .build();
            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.debug("postMultipartFile to {} failed: {}", url, e.toString());
            return false;
        }
    }
}
