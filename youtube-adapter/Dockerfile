# Base image
FROM python:3.10-alpine

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY *.py .

# Expose port 9130
EXPOSE 9130

# Start the web server
CMD ["python", "youtube-adapter.py"]
