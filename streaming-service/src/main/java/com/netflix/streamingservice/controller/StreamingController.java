package com.netflix.streamingservice.controller;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.streamingservice.dto.StreamingResponse;
import com.netflix.streamingservice.service.StreamingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;

@RestController
@RequestMapping("/api/v1/stream")
@Slf4j
@RequiredArgsConstructor
public class StreamingController {

    private final StreamingService streamingService;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String MASTER_PLAYLIST_KEY_PREFIX = "streaming:playlist";

    /**
     * Get Streaming URL for a movie
     * Returns presigned HLS master playlist URL
     * 
     * GET /api/vi/stream/{movieId}
     */
    @GetMapping("/{movieId}")
    public ResponseEntity<StreamingResponse> getStreamingUrl(
            @PathVariable String movieId) {
        log.info("Streaming request for movie: {}", movieId);

        // Get master playlist key from Redis
        String playlistKey = redisTemplate.opsForValue()
                .get(MASTER_PLAYLIST_KEY_PREFIX + movieId);

        if (playlistKey == null) {
            return ResponseEntity.notFound().build();
        }

        StreamingResponse response = streamingService.getStreamingUrl(movieId, playlistKey);

        return ResponseEntity.ok(response);
    }

    /**
     * Server signed m3u8 playlist content
     * Called by HLS player for each quality playlist
     * 
     * @param movieId
     * @param path
     * @return
     */
    @GetMapping("/{movieId}/playlist")
    public ResponseEntity<String> getSignedPlaylist(
            @PathVariable String movieId,
            @RequestParam String path) {

        String signedPlaylist = streamingService.getSignedPlaylist(movieId, path);

        return ResponseEntity.ok().header("Content-Type", "application/x-mpegURL").body(signedPlaylist);
    }

}
