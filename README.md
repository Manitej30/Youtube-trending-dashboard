




YouTube Trending Dashboard

A real-time dashboard showing the latest trending YouTube videos. Built with Spark, Kafka, PostgreSQL, and Streamlit.

Features

*Stream trending video data from YouTube API

*Store in PostgreSQL

*Real-time updates with Spark Streaming

*Interactive dashboard using Streamlit



Folder Structure

/dashboard       - Streamlit app & Dockerfile
/kafka           - Producer scripts
/spark           - Spark Streaming scripts
/config          - Config files
/docker-compose.yml




How to Run

Start PostgreSQL and Kafka using Docker.

Run producer script (kafka/producer.py) to send data to Kafka.

Run Spark Streaming (spark/spark_streaming.py) to process and store in PostgreSQL.

Start Streamlit dashboard (dashboard/dashboard.py) to visualize trending videos
