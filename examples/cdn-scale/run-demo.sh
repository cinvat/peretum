#!/bin/bash
# Build and start the CDN scale demo

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Peretum CDN Scale Demo ==="
echo ""

# Build images
echo "Building Docker images..."
docker compose build --parallel

echo ""
echo "Starting services..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

# Check health
echo ""
echo "Service status:"
docker compose ps

echo ""
echo "=== Demo Ready ==="
echo ""
echo "Services:"
echo "  Control Plane:     http://localhost:9001"
echo "  Edge US East:      http://localhost:8080 (HTTP) / https://localhost:8443 (HTTPS+HTTP/3)"
echo "  Edge US West:      http://localhost:8081 / https://localhost:8444"
echo "  Edge EU Central:   http://localhost:8082 / https://localhost:8445"
echo "  Prometheus:        http://localhost:9090"
echo "  Grafana:           http://localhost:3000 (admin/admin)"
echo ""
echo "Test commands:"
echo "  curl -I http://localhost:8080/api/users"
echo "  curl -I http://localhost:8080/live/stream.m3u8"
echo "  curl http://localhost:9090/metrics | grep peretum"
echo ""
echo "Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "To stop: docker compose down -v"