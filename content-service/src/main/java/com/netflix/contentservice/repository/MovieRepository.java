package com.netflix.contentservice.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;

public interface MovieRepository extends JpaRepository<Movie, String> {
    List<Movie> findByGenre(Genre genre);

    List<Movie> findByTitleContainingIgnoreCase(String title);
}
