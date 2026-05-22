package com.netflix.contentservice.model;

/**
 * Tracks the video processing lifecycle.
 * 
 * FLOW =>
 * PENDING -> UPLOADED -> ENCODING -> ENCODED -> READY or FAILED
 */
public enum VideoStatus {

    PENDING, // movie added but not uploaded yet
    UPLOADED, // row video uploaded to s3
    ENCODING, // FFmpeg is encoding the video
    ENCODED, // Encoding complete
    READY, // HLS playlist ready
    FAILED // Encoding Failed

}
