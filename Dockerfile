# Use lightweight Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files & enable stdout logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies (needed for aiohttp & SSL)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source code
COPY . .

# Run the bot
CMD ["python", "main.py"]
