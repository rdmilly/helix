FROM python:3.12-slim

WORKDIR /app

# Install dependencies (fastembed needs build tools for some ONNX deps)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Auth deps (separate layer for cache efficiency)
RUN pip install --no-cache-dir bcrypt==4.2.1 PyJWT==2.10.1

# Copy application
COPY . .

# Create data directory (model cache lives here on persistent volume)
RUN mkdir -p /app/data/models

# Expose port
EXPOSE 9050

# Health check -- longer start_period for first-run model download (~600MB)
HEALTHCHECK --interval=30s --timeout=10s --start-period=300s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9050/health')"

# Run application
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9050"]
