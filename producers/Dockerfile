# Use the official Python slim image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy only necessary application files

COPY .env fetch_data_argument.py ./

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["python", "fetch_data_argument.py"]