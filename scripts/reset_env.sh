#!/bin/bash
# Clean up old containers
docker compose down -v
# Start new stack

# Start new stack
docker compose up -d --build

echo "Waiting for services to be ready..."
sleep 15
docker ps
