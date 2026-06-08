package com.netflix.streamingservice.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.dto.StreamingResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {

    private final S3Presigner s3Presigner;
    private final RedisTemplate<String, String> redisTemplate;
    private final S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${aws.s3.presigned-url-expiry}")
    private long presignedUrlExpiry; // 60 minute

    // Redis key for caching streaming URLs
    private final static String STREAMING_URL_CACHE_PREFIX = "streaming:url:";

    /**
     * Get streaming URL for a movie
     *
     * FLOW:
     * 1. Check redis Cache for existing presigned URL.
     * 2. If Cached - return immediately
     * 3. If not Cached = generate new presigned URL from S3
     * 4. Cache the URL in Redis
     * 5. Return streaming URL
     * 
     * Why presigned URL?
     * - S3 bucket is private locker room - videos are not publicly accessible
     * - Presigned URL gives temporary access ( X minute)
     * - Prevent unauthorized video downloads
     */

    public StreamingResponse getStreamingUrl(String movieId, String playlistKey) {
        log.info("Getting streaming URL for movie : {}", movieId);

        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;

        // Check redis Cache first
        String cachedUrl = redisTemplate.opsForValue().get(cacheKey);

        if (cachedUrl != null) {
            log.info("Returning cached streaming URL for movie : {}", movieId);

            return new StreamingResponse(
                    movieId,
                    cachedUrl,
                    "1080p, 720p, 480p, 360p",
                    presignedUrlExpiry);
        }

        // Generate presigned URLfrom S3
        log.info("Generation new presigned for movie : {}", movieId);

        String presignedUrl = generatePresignedUrl(playlistKey);

        // [!important] Cache in redis for 55 minute
        // why? (5 minutes less that actual expiry to avoid edge cases)
        redisTemplate.opsForValue().set(cacheKey, presignedUrl, 55, TimeUnit.MINUTES);

        log.info("Streaming URL generated and cached for movie : {}", movieId);

        return new StreamingResponse(
                movieId,
                presignedUrl,
                "1080p, 720p, 480p, 360p",
                presignedUrlExpiry);
    }

    /**
     * Generate a presigned URL for S3 object
     * URL expired after configured time
     * 
     * @param playlistKey
     * @return
     */
    private String generatePresignedUrl(String key) {
        var getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        var presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
                .getObjectRequest(getObjectRequest)
                .build();

        return s3Presigner.presignGetObject(presignRequest)
                .url()
                .toString();
    }

    /**
     * Invalidate Cache streaming URL
     * Called when video is re-encoded or updated
     */

    public void invalidateCache(String movieId) {
        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
        redisTemplate.delete(cacheKey);

        log.info("Streaming URL cache invalidated for movie : {}", movieId);
    }

    /**
     * THIS is the KEY METHOD that makes EVERYTHING SECURE
     * 
     * @param movieId
     * @param playlistPath
     * @return
     */
    public String getSignedPlaylist(String movieId, String playlistPath) {
        // Get base path for this playlist
        String basePath = playlistPath.substring(0, playlistPath.lastIndexOf('/') + 1);

        // Read m3u8 content from S3
        String m3u8Content = readFromS3(playlistPath);

        // Rewrite each line that is a segment or playlist reference
        String signedContent = rewriteM3u8SignedUrls(m3u8Content, basePath);

        return signedContent;
    }

    /**
     * Read file content from S3
     * 
     * @param playlistPath
     * @return
     */
    private String readFromS3(String s3Key) {
        var request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);

        try (var isr = new InputStreamReader(response);
                var br = new BufferedReader(isr);) {

            return br.lines().collect(Collectors.joining("\n"));

        } catch (Exception e) {
            log.error("Failed to read filed from S3 for key : {}", s3Key, e);
            throw new RuntimeException("S3 파일 읽기 실패", e);
        }
    }

    private String rewriteM3u8SignedUrls(String m3u8Content, String basePath) {
        var rewritten = new StringBuilder();

        for (String line : m3u8Content.split("\n")) {
            String trimmed = line.trim();

            // Skip empty lines and comments
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                rewritten.append(line).append("\n");
                continue;
            }

            // This is a segment or playlist reference
            // Build full S3 key and Sign it

            String fullKey = basePath + trimmed;
            String signedUrl = generatePresignedUrl(fullKey);

            rewritten.append(signedUrl).append("\n");
        }

        return rewritten.toString();
    }

}
