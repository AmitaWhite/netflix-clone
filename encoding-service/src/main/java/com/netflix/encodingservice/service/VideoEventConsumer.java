package com.netflix.encodingservice.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoEventConsumer {

    private final EncodingService encodingService;

    /**
     * Listens to video.uploaded Kafka topic.
     * Triggered when video service uploads a raw video to S3.
     * 
     * FLOW:
     * 
     * Video Service -> S3 upload -> 1. Kafka (video.uploaded)
     * (Same Layer as Kafka) -> 2. This Consumer
     * (Same Layer as Kafka) -> 3. EncodingService -> FFmpeg -> S3
     * (Same Layer as Kafka) -> 4. Kafka (video.encoded)
     */

    @KafkaListener(topics = "video.uploaded", groupId = "encoding-service-group")
    public void consumeVideoUploadedEvent(VideoUploadedEvent event) {
        log.info("Consumed VideoUPloadedEvent for movie: {} \t file {}",
                event.getMovieId(),
                event.getOriginalFileName());

        try {
            encodingService.encodeVideo(event);
        } catch (Exception e) {
            log.error("Failed to process encoding for movie: {} - {}", event.getMovieId(), e.getMessage());
        }

    }
}
