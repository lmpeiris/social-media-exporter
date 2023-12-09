# Base image
FROM python:3.11-alpine

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY *.py .

# Start the web server
CMD ["python", "advanced-scheduler.py"]
