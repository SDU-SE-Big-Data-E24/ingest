# Use the official Python slim image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy only necessary application files
COPY consumer.py .env consumption_industry_schema.avsc ./

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "consumer.py"]
