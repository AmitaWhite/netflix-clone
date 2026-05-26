package com.netflix.videoservice.service;

import java.io.IOException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.event.VideoUploadedEvent;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * TODO: [001] SOLID 원칙에 맞는 코드 개선점
 * 
 * 1. SRP(Single Responsibility Principle) - 미흡
 * 2. OCP(Open-Close Principle) - 미흡
 * 3. LSP(Liskov Substitution Principle) - 준수
 * 4. ISP (인터페이스 분리 원칙) — 준수
 * 5. DIP (의존역전 원칙) — 미흡
 */

@Service
@Slf4j
@AllArgsConstructor
public class VideoService {

    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoUploadedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    private static final String VIDEO_UPLOADED_TOPIC = "video.uploaded";

    /**
     * Upload video to AWS s3 and publish VideoUploadedEvent to Kafka
     * 
     * Flow:
     * 1. Receive multipart video file
     * 2. Generate unique S3 key
     * 3. Upload to S3
     * 4. Publish VideoUploadedEvent to Kafka
     * 5. Encoding Service picks up and start FFmpeg
     * 
     * @throws IOException
     */

    public String uploadVideo(String movieId, MultipartFile file) throws IOException {
        log.info("Starting video upload for movie : {} file : {}", movieId, file.getOriginalFilename());

        // Generate unique S3 key for raw video
        // Format : raw/movieId/uuid_filename

        String videoKey = "raw/" + movieId + "/" + UUID.randomUUID() + "_" + file.getOriginalFilename();

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .key(videoKey)
                .contentType(file.getContentType())
                .contentLength(file.getSize())
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));

        log.info("video uploaded to S3");

        // Publish event to Kafka
        // Encoding Service will consume this and start FFmpeg processing

        VideoUploadedEvent event = new VideoUploadedEvent(
                movieId,
                videoKey,
                bucketName,
                file.getOriginalFilename(),
                file.getSize());

        kafkaTemplate.send(VIDEO_UPLOADED_TOPIC, movieId, event);

        log.info("VideoUploadedEvent published for movie : {}", movieId);

        return videoKey;
    }

}
