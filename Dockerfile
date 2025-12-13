FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy all project files (including .env) into the image
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Cloud Run uses this port
ENV PORT=8080
EXPOSE 8080

# Start your Flask app
CMD ["python", "coingecko.py"]
