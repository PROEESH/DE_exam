# Use official Python runtime
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependencies file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run the main script
CMD ["python", "main.py"]
