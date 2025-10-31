# Use an official lightweight Ubuntu image
FROM ubuntu:22.04

# Avoid user interaction during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install required tools
RUN apt-get update && apt-get install -y \
    mysql-client \
    bash \
    curl \
    nano \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy your SQL and Bash scripts into the container
COPY . /app

# Make your ETL script executable
RUN chmod +x /app/setup_etl.sh

# Set default command (optional: run ETL automatically)
CMD ["/app/setup_etl.sh"]
