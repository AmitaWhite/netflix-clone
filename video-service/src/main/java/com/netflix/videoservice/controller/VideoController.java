package com.netflix.videoservice.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.service.VideoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/videos")
@Slf4j
@RequiredArgsConstructor
public class VideoController {

    private final VideoService videoService;

    private static final int KB_SIZE = 2 ^ 10;
    private static final int MB_SIZE = KB_SIZE * 2 ^ 10;

    /**
     * Upload video file for a movie
     * Accepts multipart file upload
     * 
     * POST /api/v1/videos/upload/{movieId}
     */

    public ResponseEntity<String> uploadVideo(@PathVariable String movieId, @RequestParam("file") MultipartFile file)
            throws IOException {
        log.info("Video upload request for video : {}\t file size : {}MB", movieId, file.getSize() / MB_SIZE);

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("File is empty");
        }

        String videoKey = videoService.uploadVideo(movieId, file);
        return ResponseEntity.ok(
                "Video uploaded successfully! Key : " + videoKey
                        + " - Encoding started automatically via Kafka");
    }

}
