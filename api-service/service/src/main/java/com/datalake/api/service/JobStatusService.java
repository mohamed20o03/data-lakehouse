package com.datalake.api.service;

import com.datalake.api.model.UploadJob;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobStatusService {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String JOB_PREFIX = "job:";
    private static final long JOB_TTL_HOURS = 1;

    /**
     * Save job status to Redis
     * Key format: job:{jobId}
     * TTL: 1 hour
     */
    public void saveJobStatus(UploadJob job) {
        try {
            String key = JOB_PREFIX + job.getJobId();
            String value = objectMapper.writeValueAsString(job);
            
            log.info("Saving job status to Redis - Key: {}, Status: {}", key, job.getStatus());
            
            // Save with TTL (Time To Live)
            redisTemplate.opsForValue().set(key, value, JOB_TTL_HOURS, TimeUnit.HOURS);
            
            log.debug("Job status saved successfully: {}", job.getJobId());
            
        } catch (Exception e) {
            log.error("Failed to save job status to Redis - JobId: {}", job.getJobId(), e);
            // Don't throw exception - job can still proceed even if Redis fails
        }
    }

    /**
     * Get job status from Redis
     */
    public UploadJob getJobStatus(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            
            log.debug("Retrieving job status from Redis - Key: {}", key);
            
            String value = redisTemplate.opsForValue().get(key);
            
            if (value == null) {
                log.warn("Job not found in Redis: {}", jobId);
                return null;
            }
            
            UploadJob job = objectMapper.readValue(value, UploadJob.class);
            
            log.info("Job status retrieved - JobId: {}, Status: {}", jobId, job.getStatus());
            
            return job;
            
        } catch (Exception e) {
            log.error("Failed to get job status from Redis - JobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * Update job status (typically called by worker)
     */
    public void updateJobStatus(String jobId, String status, String message) {
        try {
            String key = JOB_PREFIX + jobId;
            
            log.info("Updating job status - JobId: {}, Status: {}, Message: {}", 
                     jobId, status, message);
            
            // Get existing job
            UploadJob job = getJobStatus(jobId);
            
            if (job != null) {
                // Update fields
                job.setStatus(status);
                job.setMessage(message);
                
                // Save back to Redis
                saveJobStatus(job);
                
                log.info("Job status updated successfully: {}", jobId);
            } else {
                log.warn("Cannot update job status - job not found: {}", jobId);
            }
            
        } catch (Exception e) {
            log.error("Failed to update job status - JobId: {}", jobId, e);
        }
    }

    /**
     * Delete job status from Redis
     */
    public void deleteJobStatus(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            
            log.info("Deleting job status from Redis: {}", jobId);
            
            Boolean deleted = redisTemplate.delete(key);
            
            if (Boolean.TRUE.equals(deleted)) {
                log.info("Job status deleted successfully: {}", jobId);
            } else {
                log.warn("Job status not found or already deleted: {}", jobId);
            }
            
        } catch (Exception e) {
            log.error("Failed to delete job status - JobId: {}", jobId, e);
        }
    }

    /**
     * Check if job exists in Redis
     */
    public boolean jobExists(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            Boolean exists = redisTemplate.hasKey(key);
            return Boolean.TRUE.equals(exists);
        } catch (Exception e) {
            log.error("Error checking job existence - JobId: {}", jobId, e);
            return false;
        }
    }
}