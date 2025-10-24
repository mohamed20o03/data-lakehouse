package com.datalake.spark;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;

public class WorkerApp {
    private static final Logger log = LoggerFactory.getLogger(WorkerApp.class);
    private static final String QUEUE = System.getenv().getOrDefault("RABBITMQ_QUEUE", "file.processing.queue");

    public static void main(String[] args) throws Exception {
        log.info("Starting Spark worker app");

        // Initialize SparkSession (Iceberg extensions configured via spark-defaults.conf in image)
        SparkSession spark = SparkSession.builder()
                .appName("datalake-worker")
                .getOrCreate();

        // MinIO client
        MinioClient minio = MinioClient.builder()
                .endpoint(System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000"))
                .credentials(System.getenv().getOrDefault("MINIO_ACCESS_KEY", "admin"),
                        System.getenv().getOrDefault("MINIO_SECRET_KEY", "password123"))
                .build();

        // RabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(System.getenv().getOrDefault("RABBITMQ_HOST", "rabbitmq"));
        factory.setPort(Integer.parseInt(System.getenv().getOrDefault("RABBITMQ_PORT", "5672")));
        factory.setUsername(System.getenv().getOrDefault("RABBITMQ_USER", "admin"));
        factory.setPassword(System.getenv().getOrDefault("RABBITMQ_PASS", "password123"));

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare(QUEUE, true, false, false, null);

        ObjectMapper mapper = new ObjectMapper();
        // tolerate extra fields from the producer (e.g., userId, status, message)
        mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String payload = new String(delivery.getBody());
                log.info("Received message: {}", payload);

                // Parse a simple job JSON (jobId, filePath, tableName)
                JobMessage job = mapper.readValue(payload, JobMessage.class);

                // Download object from MinIO
                Path tmpDir = Files.createTempDirectory("spark-worker-");
                Path outPath = tmpDir.resolve(job.fileName != null ? job.fileName : "upload");

                try (InputStream in = minio.getObject(GetObjectArgs.builder().bucket(System.getenv().getOrDefault("MINIO_UPLOADS_BUCKET", "uploads")).object(job.filePath).build());
                     FileOutputStream fos = new FileOutputStream(outPath.toFile())) {
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = in.read(buf)) != -1) {
                        fos.write(buf, 0, r);
                    }
                }

                // Read file into DataFrame
                String lower = outPath.toString().toLowerCase();
                Dataset<Row> df;
                if (lower.endsWith(".csv")) {
                    df = spark.read().option("header", "true").csv(outPath.toString());
                } else if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) {
                    // For XLSX files, we'll need to convert them to CSV first using POI
                    // For now, throw an error to indicate this needs implementation
                    throw new UnsupportedOperationException("XLSX/XLS file processing not yet implemented. Please convert to CSV format.");
                } else {
                    throw new IllegalArgumentException("Unsupported file type: " + outPath.toString());
                }

                // Write DataFrame to Iceberg table via catalog 'local.db.table'
                String table = job.tableName != null && !job.tableName.isBlank() ? job.tableName : "default_table";
                String full = String.format("local.db.%s", table);

                df.writeTo(full)
                        .using("iceberg")
                        .tableProperty("format-version", "2")
                        .createOrReplace();

                log.info("Wrote {} rows to Iceberg table {}", df.count(), full);

                // Acknowledge
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                // cleanup
                try { Files.deleteIfExists(outPath); Files.deleteIfExists(tmpDir); } catch (Exception ignored) {}

            } catch (Exception e) {
                log.error("Failed to process message", e);
                try { channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false); } catch (Exception ex) { log.error("Failed to nack", ex); }
            }
        };

        channel.basicConsume(QUEUE, false, deliverCallback, consumerTag -> {});

        // Keep the process running
        Thread.currentThread().join();
    }

    static class JobMessage {
        public String jobId;
        public String filePath;
        public String fileName;
        public String tableName;
    }
}
