package com.datalake.api.controller;

import com.datalake.api.model.UploadJob;
import com.datalake.api.service.FileStorageService;
import com.datalake.api.service.JobStatusService;
import com.datalake.api.service.RabbitMQService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Controller that accepts file uploads and enqueues processing jobs.
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
@RequiredArgsConstructor
public class FileUploadController {

	private final FileStorageService storageService;
	private final JobStatusService jobStatusService;
	private final RabbitMQService rabbitMQService;

	@PostMapping("/upload")
	public ResponseEntity<?> uploadFile(@RequestParam("file") MultipartFile file,
										@RequestParam(value = "userId", required = false) String userId,
										@RequestParam(value = "tableName", required = false) String tableName) {
		try {
			if (file == null || file.isEmpty()) {
				return ResponseEntity.badRequest().body(Map.of("error", "file is required"));
			}
			// generate jobId first so we can use it in the object path
			String jobId = UUID.randomUUID().toString();

			// store the file to MinIO under uploads/{jobId}/<filename>
			String storedPath = storageService.storeFile(file, jobId);

			// create job object
			UploadJob job = UploadJob.builder()
					.jobId(jobId)
					.userId(userId != null ? userId : "anonymous")
					.fileName(file.getOriginalFilename())
					.filePath(storedPath)
					.tableName(tableName != null ? tableName : "")
					.fileSize(file.getSize())
					.timestamp(LocalDateTime.now())
					.status("queued")
					.message("File received and queued")
					.build();

			// persist job status and enqueue
			jobStatusService.saveJobStatus(job);
			rabbitMQService.sendJob(job);

			Map<String, Object> resp = new HashMap<>();
			resp.put("jobId", jobId);
			resp.put("status", "queued");

			return ResponseEntity.status(HttpStatus.ACCEPTED).body(resp);

		} catch (Exception e) {
			log.error("Upload failed", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(Map.of("error", "upload failed", "details", e.getMessage()));
		}
	}
}
