FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

# Copy shared modules and both services
COPY shared/ /app/shared/
COPY services/webhook/ /app/services/webhook/
COPY services/sync/ /app/services/sync/

# Install shared dependencies
RUN pip install --no-cache-dir -r /app/shared/requirements.txt

# Install webhook dependencies
RUN pip install --no-cache-dir -r /app/services/webhook/requirements.txt

# Install sync dependencies
RUN pip install --no-cache-dir -r /app/services/sync/requirements.txt

# Install web server dependencies for webhook
RUN pip install --no-cache-dir fastapi uvicorn python-multipart

# Set Python path to include shared modules
ENV PYTHONPATH=/app/shared

EXPOSE 8080

# Default CMD runs the webhook service
# Override with --command at deploy time to run the sync job
CMD ["uvicorn", "services.webhook.main:app", "--host", "0.0.0.0", "--port", "8080"]
# cache-bust

# force rebuild 2026-04-22T22:50:07.410495

# force rebuild 2026-04-23T09:56:21.410079
