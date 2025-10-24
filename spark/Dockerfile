FROM tabulario/spark-iceberg:latest

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

# Set working directory
WORKDIR /opt/spark
