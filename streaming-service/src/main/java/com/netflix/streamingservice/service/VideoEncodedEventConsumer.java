package com.netflix.streamingservice.service;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.event.VideoEncodedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoEncodedEventConsumer {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String MASTER_PLAYLIST_KEY_PREFIX = "streaming:playlist";

    /**
     * Listens to video.encoded Kafka topic.
     * Stores master playlist key is Redis when encoding is complete
     * This allows StreamingService to quickly find the playlist key by movieId.
     */

    @KafkaListener(topics = "video.encoded", groupId = "streaming-service-group")
    public void consumeVideoEncoded(VideoEncodedEvent event) {
        log.info("Consumed VideoEncodedEvent for movie : {} success : {}", event.getMovieId(), event.isSuccess());

        if (event.isSuccess()) {
            // Store master playlist key in redis
            String cacheKey = MASTER_PLAYLIST_KEY_PREFIX + event.getMovieId();
            redisTemplate.opsForValue().set(cacheKey, event.getMasterPlaylistKey());
        } else {
            log.error("Encoding failed for movie : {}, {}", event.getMovieId(), event.getErrorMessage());
        }
    }
}
