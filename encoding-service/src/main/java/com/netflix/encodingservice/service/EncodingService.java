package com.netflix.encodingservice.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import javax.management.RuntimeErrorException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {

    // TODO:[002] refactoring here

    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            new int[] { 1_920, 5_000, 1_080 }, // 1080p - 5000k bitrate
            new int[] { 1_280, 2_800, 720 }, // 720p - 2800k bitrate
            new int[] { 854, 1_200, 480 }, // 480p - 1200k bitrate
            new int[] { 640, 800, 360 } // 360p - 800k bitrate
    );

    private final S3Client s3Client;

    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    // Video qualities to encode
    // Format : resolution , bitrate, height

    @Value("${encoding.base-path}")
    private String basePath;

    /**
     * Main encoding pipeline
     * Steps :
     * 1. Download raw video from S3
     * 2. Encode to multiple qualities using FFmpeg
     * 3. Generate HLS playlist (.m3u8) for each quality
     * 4. Create master playlist
     * 5. Upload all encoded files back to S3
     * 6. Publish VideoEncodedEvent to Kafka
     * 
     * @param event
     */
    public void encodeVideo(VideoUploadedEvent event) {
        log.info("Starting encoding platform for movie : {}", event.getMovieId());

        // Create a unique path for a movie
        String jobPath = basePath + "/" + event.getMovieId();

        try {
            // Create temp directories
            Files.createDirectories(Paths.get(jobPath));
            Files.createDirectories(Paths.get(jobPath + "/encoded"));

            // Step 1: Download raw video from S3
            String localVideoPath = jobPath + "/raw_video.mp4";
            downloadFromS3(event.getVideoKey(), localVideoPath);

            log.info("Raw video downloaded to : {}", localVideoPath);

            // Step 2: Encode to multiple qualities + generate HLS
            for (int[] qualities : VIDEO_QUALITIES) {
                int width = qualities[0];
                int bitrate = qualities[1];
                int height = qualities[2];

                String qualityDir = jobPath + "/encoded" + height + "p";
                Files.createDirectories(Paths.get(qualityDir));

                encodeToHLS(localVideoPath, qualityDir, width, height, bitrate);

                log.info("Encoded {}p successfully", height);

            }

            // Step 4: Generate master playlist
            String masterPlaylistPath = jobPath + "/encoded/master.m3u8";
            generateMasterPlaylist(masterPlaylistPath);

            log.info("Master Playlist generated");

            // Step 5: Upload all resources file to S3
            String encodedPrefix = "encoded/" + event.getMovieId() + "/";
            uploadEncodedFilesToS3(jobPath + "/encoded", encodedPrefix);

            log.info("All encoded files uploaded to S3");

            // Step 6: Publish VideoEncodedEvent
            String masterPlaylistKey = encodedPrefix + "master.m3u8";
            String hlsUrl = "https://" + bucketName + ".s3.amazonaws.com/" + masterPlaylistKey;

            VideoEncodedEvent encodedEvent = new VideoEncodedEvent(
                    event.getMovieId(),
                    hlsUrl,
                    masterPlaylistKey,
                    true,
                    null);

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), encodedEvent);
            log.info("VideoEncodedEvent published for movie : {}", event.getMovieId());

        } catch (Exception e) {
            log.error("Encoding failed for movie : {} - {} ", event.getMovieId(), e.getMessage());

            // Publish failure event;
            VideoEncodedEvent failureEvent = new VideoEncodedEvent(
                    event.getMovieId(),
                    null,
                    null,
                    false,
                    e.getMessage());

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), failureEvent);

        } finally {
            // cleanup temp files
            cleanupTempFiles(jobPath);
        }

    }

    /**
     * Encode video to HLS using FFmpeg
     * 
     * FFmpeg command created:
     * - Multiple .ts segment files (10 seconds each)
     * - A .m3u8 playlist file for this quality
     *
     * @param inputPath
     * @param outputDir
     * @param width
     * @param height
     * @param bitrate
     * @throws IOException
     * @throws InterruptedException
     */
    private void encodeToHLS(String inputPath, String outputDir, int width, int height, int bitrate)
            throws IOException, InterruptedException {
        String playlistPath = outputDir + "/playlist.m3u8";
        String segmentPattern = outputDir + "/segment_%03d.ts";

        // FFmpeg Command for HLS encoding
        List<String> command = Arrays.asList(
                ffmpegPath,
                "-i", inputPath, // Input file
                "-vf", "scale=" + width + ":" + height, // Scale to resolution
                "-c:v", "libx264", // video codec
                "-b:v", bitrate + "k", // video bitrate
                "-c:a", "aac", // Audio coded
                "-b:a", "128k", // Audio bitrate
                "-hls_time", "10", // 10 second segments
                "-hls_list_size", "0", // Keep all segments
                "-hls_segment_filename", segmentPattern, // segment naming
                "-f", "hls", // output format HLS
                playlistPath // output playlist
        );

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeErrorException(null, "FFmpeg encoding failed with exit code : " + exitCode);
        }
    }

    /**
     * Clean up temp files after encoded
     * 
     * @param jobPath
     */
    private void cleanupTempFiles(String jobPath) {
        try {
            Path dirPath = Paths.get(jobPath);
            if (Files.exists(dirPath)) {
                Files.walk(dirPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);

                log.info("Temp files cleaned up for job: {}", jobPath);
            }
        } catch (IOException e) {
            log.warn("Failed to cleanup temp files : {}", e.getMessage());
        }

    }

    /**
     * Upload all encoded files from local directory back to S3
     * 
     * @param localDir
     * @param s3Prefix
     * @throws IOException
     */
    private void uploadEncodedFilesToS3(String localDir, String s3Prefix) throws IOException {
        File directory = new File(localDir);
        uploadDirectoryToS3(directory, localDir, s3Prefix);

    }

    private void uploadDirectoryToS3(File dir, String baseDir, String s3Prefix) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                uploadDirectoryToS3(dir, baseDir, s3Prefix);
            } else {
                String relativePath = file.getAbsolutePath()
                        .substring(baseDir.length() + 1)
                        .replace("\\", "/");

                String s3Key = s3Prefix + relativePath;
                String contentType = file.getName().endsWith(".m3u8")
                        ? "application/x-mpegURL"
                        : "video/MP2T";

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .contentType(contentType)
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));

                log.debug("Uploaded : {}", s3Key);
            }
        }

    }

    /**
     * Generate master HLS playlist that references all quality playlists.
     * This is the file the video player downloads first.
     * 
     * @param masterPlaylistPath
     * @throws IOException
     */
    private void generateMasterPlaylist(String masterPlaylistPath) throws IOException {
        StringBuilder master = new StringBuilder();
        master.append("#EXTM3U\n"); // extended M3U
        master.append("#EXT-X-VERSION:3\n\n");

        // Add each quality to master playlist
        int[][] qualities = {
                { 1_920, 5_000, 1_000 },
                { 1_280, 2_800, 720 },
                { 854, 1_200, 480 },
                { 640, 800, 360 } };

        for (int[] q : qualities) {
            int width = q[0];
            int bitrate = q[1];
            int height = q[2];

            master.append("#EXT-X-STREAM-INF:BADWIDTH=")
                    .append(bitrate * 1_000)
                    .append(", RESOLUTION=").append(width).append("x").append(height)
                    .append(", CODECS=\"avc1.42e01e,mp4a.40.2\"\n");

            master.append(height).append("p/playlist.m3u8\n\n");
        }

        Files.writeString(Paths.get(masterPlaylistPath), master.toString());
    }

    private void downloadFromS3(String s3Key, String localVideoPath) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.getObject(getObjectRequest, Paths.get(localVideoPath));
    }

}
