FROM maven:3.9.4-eclipse-temurin-17 AS builder
WORKDIR /build
COPY worker-app/pom.xml ./
COPY worker-app/src ./src
RUN mvn -DskipTests package

FROM tabulario/spark-iceberg:latest
WORKDIR /opt/spark/worker-app
COPY --from=builder /build/target/worker-app-0.0.1-SNAPSHOT-shaded.jar ./worker-app.jar

# Install PostgreSQL client as root
USER root
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy PostgreSQL JDBC driver
COPY jars/postgresql-42.7.8.jar /opt/spark/jars/

# Copy Spark configuration
COPY conf/spark-defaults.conf /opt/spark/conf/

# Create a custom entrypoint script
RUN echo '#!/bin/bash\n\
# Start the worker app using spark-submit\n\
/opt/spark/bin/spark-submit \\\n\
  --class com.datalake.spark.WorkerApp \\\n\
  --master local[*] \\\n\
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \\\n\
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \\\n\
  --conf spark.sql.catalog.local.type=jdbc \\\n\
  --conf spark.sql.catalog.local.uri=jdbc:postgresql://postgres:5432/iceberg_catalog \\\n\
  --conf spark.sql.catalog.local.jdbc.user=iceberg_user \\\n\
  --conf spark.sql.catalog.local.jdbc.password=iceberg_pass \\\n\
  --conf spark.sql.catalog.local.warehouse=s3a://warehouse/wh/ \\\n\
  --conf spark.sql.catalog.local.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\\n\
  --conf spark.sql.catalog.local.s3.endpoint=http://minio:9000 \\\n\
  --conf spark.sql.catalog.local.s3.path-style-access=true \\\n\
  --conf spark.sql.catalog.local.s3.access-key-id=admin \\\n\
  --conf spark.sql.catalog.local.s3.secret-access-key=password123 \\\n\
  --conf spark.sql.catalog.local.client.region=us-east-1 \\\n\
  --conf spark.sql.defaultCatalog=local \\\n\
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\\n\
  --conf spark.hadoop.fs.s3a.access.key=admin \\\n\
  --conf spark.hadoop.fs.s3a.secret.key=password123 \\\n\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\\n\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\\n\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\\n\
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\\n\
  --conf spark.hadoop.fs.s3a.region=us-east-1 \\\n\
  /opt/spark/worker-app/worker-app.jar\n\
' > /opt/spark/worker-app/entrypoint.sh && \
    chmod +x /opt/spark/worker-app/entrypoint.sh

ENV SPARK_HOME=/opt/spark
ENTRYPOINT ["/opt/spark/worker-app/entrypoint.sh"]