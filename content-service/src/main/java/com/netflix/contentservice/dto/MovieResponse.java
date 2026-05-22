package com.netflix.contentservice.dto;

import java.time.LocalDateTime;

import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;

public record MovieResponse(
        String id,
        String title,
        String description,
        Genre genre,
        String director,
        String cast,
        int releaseYear,
        double rating,
        String thumbnailUrl,
        int durationMinute,
        String videoKey,
        String hlsUrl,
        VideoStatus videoStatus,
        LocalDateTime createAt) {

    public static MovieResponse from(Movie movie) {
        return new MovieResponse(
                movie.getId(),
                movie.getTitle(),
                movie.getDescription(),
                movie.getGenre(),
                movie.getDirector(),
                movie.getCast(),
                movie.getReleaseYear(),
                movie.getRating(),
                movie.getThumbnailUrl(),
                movie.getDurationMinute(),
                movie.getVideoKey(),
                movie.getHlsUrl(),
                movie.getVideoStatus(),
                movie.getCreateAt());
    }
}
