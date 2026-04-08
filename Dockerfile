# Use nginx alpine for minimal size — supports ARM64 (Raspberry Pi)
FROM nginx:alpine

# Copy HTML app
COPY bibbloard.html /usr/share/nginx/html/index.html

# Copy pre-generated data directory (JSON files only, no CSVs)
COPY data/*.json /usr/share/nginx/html/data/

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Install curl for health checks
RUN apk add --no-cache curl

# Expose port 80
EXPOSE 80

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost/health || exit 1
