package com.datalake.api.controller;

import com.datalake.api.model.UploadJob;
import com.datalake.api.service.JobStatusService;
import com.datalake.api.service.RabbitMQService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * JobController handles endpoints related to job tracking and queue monitoring.
 * 
 * Responsibilities:
 *  - Retrieve the current status of a specific upload job.
 *  - Fetch RabbitMQ queue statistics (message and consumer counts).
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
@RequiredArgsConstructor
public class JobController {

    // Service that stores and retrieves job status information
    private final JobStatusService jobStatusService;

    // Service that interacts with RabbitMQ to get queue statistics
    private final RabbitMQService rabbitMQService;

    /**
     * GET /api/v1/jobs/{jobId}
     * 
     * Retrieves the current status of a specific job by its ID.
     * 
     * @param jobId the unique identifier of the upload job
     * @return 200 OK with job details if found, or 404 if not found
     */
    @GetMapping("/jobs/{jobId}")
    public ResponseEntity<?> getJobStatus(@PathVariable String jobId) {
        UploadJob job = jobStatusService.getJobStatus(jobId);
        if (job == null) {
            // If job is not found, return 404 response with an error message
            return ResponseEntity.status(404).body(Map.of("error", "job not found"));
        }
        // If found, return job details (status, filename, timestamp, etc.)
        return ResponseEntity.ok(job);
    }

    /**
     * GET /api/v1/queue/stats
     * 
     * Returns basic statistics about the RabbitMQ queue such as:
     * - queue name
     * - number of pending messages
     * - number of connected consumers
     * 
     * @return 200 OK with queue statistics in JSON format
     */
    @GetMapping("/queue/stats")
    public ResponseEntity<?> getQueueStats() {
        Map<String, Object> stats = rabbitMQService.getQueueStats();
        return ResponseEntity.ok(stats);
    }
}
