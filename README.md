<img width="1693" height="929" alt="image" src="https://github.com/user-attachments/assets/df83678f-7d33-44e3-a8d7-f9bbdbbb293b" />







# YouTube Real-Time Analytics Pipeline

A production-style real-time data engineering project that streams live YouTube trending data using Apache Kafka, processes data with Apache Spark Structured Streaming, stores records in PostgreSQL, and visualizes analytics through a modern Streamlit dashboard.

---

# Tech Stack

* Python
* Apache Kafka
* Apache Spark
* PostgreSQL
* Streamlit
* Apache Airflow
* Docker

---

# Architecture

```text id="8gc5vq"
YouTube API
    ↓
Kafka Producer
    ↓
Apache Kafka
    ↓
Spark Structured Streaming
    ↓
PostgreSQL
    ↓
Streamlit Dashboard
```

---

# Features

* Real-time YouTube data ingestion
* Kafka streaming pipeline
* Spark Structured Streaming processing
* PostgreSQL live storage
* Interactive analytics dashboard
* Airflow DAG orchestration
* Dockerized environment
* Live event tracking
* Kafka ingestion counter
* Auto-refresh dashboard
* Real-time metrics & analytics

---


---

# Project Structure

```text id="m8zzn3"
Youtube-trending-dashboard/
│
├── airflow/
├── dashboard/
├── spark-app/
├── config/
├── docker-compose.yml
└── README.md
```

---

# Real-Time Dashboard Includes

* Total Views
* Total Likes
* Total Comments
* Stream Records
* Kafka Message Counter
* Latest Batch Tracking
* Top Trending Videos
* Top Channels
* Live Streaming Events

---

# What I Learned

* Real-time data streaming
* Kafka event pipelines
* Spark Structured Streaming
* PostgreSQL integration
* Airflow orchestration
* Docker containerization
* End-to-end data engineering architecture

---

# Author

### Manitej Narendula

* GitHub: `https://github.com/Manitej30`
* LinkedIn: `https://linkedin.com/in/manitej-narendula-054b87292`
