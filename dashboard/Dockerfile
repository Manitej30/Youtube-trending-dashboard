# Use a lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy dashboard script into the container
COPY dashboard.py .

# Install required Python packages
RUN pip install streamlit psycopg2-binary pandas

# Run the Streamlit app
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
