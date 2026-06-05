package com.netflix.contentservice.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;
import com.netflix.contentservice.repository.MovieRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ContentService {
    private final MovieRepository movieRepository;

    /**
     * Add a new Movie to the Catalog
     * Video is not uploaded yet at this stage (after initialize)
     * 
     * @param movieRequest
     * @return
     */
    public MovieResponse addMovie(MovieRequest movieRequest) {
        log.info("Adding new movie : {}", movieRequest.getTitle());

        Movie movie = Movie.builder()
                .title(movieRequest.getTitle())
                .description(movieRequest.getDescription())
                .genre(movieRequest.getGenre())
                .director(movieRequest.getDirector())
                .cast(movieRequest.getCast())
                .releaseYear(movieRequest.getReleaseYear())
                .rating(movieRequest.getRating())
                .thumbnailUrl(movieRequest.getThumbnailUrl())
                .durationMinute(movieRequest.getDurationMinute())
                .videoStatus(VideoStatus.PENDING)
                .build();

        Movie savedMovie = movieRepository.save(movie);

        log.info("Movie add with Id : {}", savedMovie.getId());

        return MovieResponse.from(savedMovie);
    }

    /**
     * Get all movies in th catalog
     * 
     * @return List of MovieResponse
     */
    public List<MovieResponse> getAllMovies() {
        return movieRepository
                .findAll()
                .stream()
                .map(MovieResponse::from)
                .toList();
    }

    /**
     * @param movieId
     * @return
     */
    public MovieResponse getMovieById(String movieId) {
        return movieRepository
                .findById(movieId)
                .map(MovieResponse::from)
                .orElseThrow(RuntimeException::new);
    }

    /**
     * Get Movies by genre
     * 
     * @param genre
     * @return List of MovieResponse
     */
    public List<MovieResponse> getMoviesByGenre(Genre genre) {
        return movieRepository
                .findByGenre(genre)
                .stream()
                .map(MovieResponse::from)
                .toList();
    }

    /**
     * Search Movies by Title
     * 
     * @param title
     * @return
     */
    public List<MovieResponse> searchMovies(String title) {
        return movieRepository
                .findByTitleContainingIgnoreCase(title)
                .stream()
                .map(MovieResponse::from)
                .toList();
    }

    public void updateVideoKey(String movieId, String videoKey) {
        log.info("Updating videoKey for movie", movieId);

        Movie movie = movieRepository
                .findById(movieId)
                .orElseThrow(RuntimeException::new);

        movie.setVideoKey(videoKey);
        movie.setVideoStatus(VideoStatus.UPLOADED);

        movieRepository.save(movie);
    }

    public void updateHlsUrl(String movieId, String hlsUrl) {
        log.info("Updating HLS URL for movie : {}", movieId);

        Movie movie = movieRepository
                .findById(movieId)
                .orElseThrow(RuntimeException::new);

        movie.setHlsUrl(hlsUrl);
        movie.setVideoStatus(VideoStatus.READY);

        log.info("Movie {} is now ready for Streaming", movieId);

        movieRepository.save(movie);
    }

    public void updateVideoStatus(String movieId, VideoStatus videoStatus) {
        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(RuntimeException::new);

        movie.setVideoStatus(videoStatus);
        movieRepository.save(movie);
    }
}
