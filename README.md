# Real-Time Data Pipeline with Kafka and Spark Streaming

This project implements a foundational real-time data pipeline that ingests user activity events using Apache Kafka and processes them with Spark Structured Streaming. The pipeline handles windowed aggregations, stateful transformations, and late data using watermarks, outputting results to multiple destinations including PostgreSQL, a Parquet data lake, and a Kafka enriched topic.

## Architecture

1.  **Data Generation**: A Python script (`producer.py`) simulates user activity events (page views, clicks, sessions) and publishes them to the `user_activity` Kafka topic.
2.  **Ingestion**: Apache Kafka serves as the durable event streaming platform.
3.  **Processing**: A PySpark application (`streaming_app.py`) consumes the data, parses JSON, and performs:
    *   **Tumbling Windows**: 1-minute aggregations for page view counts.
    *   **Sliding Windows**: 5-minute aggregations for active user counts.
    *   **Stateful Processing**: Logic to calculate user session durations.
    *   **Watermarking**: Handles late data by dropping events older than 2 minutes.
4.  **Sinks**:
    *   **PostgreSQL**: Stores aggregated insights for live dashboards.
    *   **Data Lake**: Stores raw/transformed events in Parquet format, partitioned by date.
    *   **Kafka**: Publishes enriched events to the `enriched_activity` topic.

## Prerequisites

-   Docker and Docker Compose
-   Python 3.x (for running the producer script)
-   `kafka-python` library (for the producer)

## Setup and Running

1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd Streaming_DataPipeline
    ```

2.  **Configure environment variables**:
    Create a `.env` file from the example:
    ```bash
    cp .env.example .env
    ```
    (On Windows: `copy .env.example .env`)

3.  **Start the infrastructure**:
    ```bash
    docker-compose up -d
    ```
    This will start Zookeeper, Kafka, PostgreSQL, and the Spark application.

4.  **Wait for health checks**:
    Ensure all containers are healthy:
    ```bash
    docker-compose ps
    ```

5.  **Run the Data Producer**:
    Install dependencies and start the producer to stream events:
    ```bash
    pip install kafka-python
    python producer.py
    ```

## Verification

### 1. Check PostgreSQL Data
Connect to the database and query the tables:
```bash
docker exec -it db psql -U user -d stream_data
```
Queries:
```sql
SELECT * FROM page_view_counts LIMIT 10;
SELECT * FROM active_users LIMIT 10;
SELECT * FROM user_sessions LIMIT 10;
```

### 2. Check Data Lake
Verify Parquet files in the local directory:
```bash
ls -R data/lake
```

### 3. Check Enriched Kafka Topic
Consume from the enriched topic:
```bash
docker exec -it kafka /usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic enriched_activity --from-beginning
```

## Project Structure

-   `docker-compose.yml`: Defines all services.
-   `init-db.sql`: Database schema initialization.
-   `producer.py`: Standalone Python script for data simulation.
-   `spark/`:
    -   `Dockerfile`: Custom Spark environment.
    -   `app/streaming_app.py`: Main PySpark streaming logic.
-   `data/lake/`: Local mount for Parquet files.

## Optimization and Best Practices
-   **Watermarking**: Implemented on the `event_time` column with a 2-minute threshold to handle late data.
-   **Idempotency**: PostgreSQL tables use primary keys to prevent duplicate records during retries.
-   **Partitioning**: Parquet files are partitioned by `event_date` for efficient historical analysis.
