# Apache Spark Cluster Setup

A Docker Compose setup for Apache Spark cluster with 1 master and 2 worker nodes.

## Quick Start

1. **Start the cluster:**

   ```bash
   docker-compose up -d
   ```

2. **Check status:**

   ```bash
   docker ps
   ```

3. **Access Web UIs:**

   - Master UI: http://localhost:8080
   - Worker 1 UI: http://localhost:8081  
   - Worker 2 UI: http://localhost:8082

4. **Stop the cluster:**

   ```bash
   docker-compose down
   ```

## Cluster Configuration

- **Master**: 1 node (ports 7077, 8080)
- **Workers**: 2 nodes (1GB RAM, 1 core each)
- **Network**: Isolated Docker network
- **Security**: Disabled for development

## Files

- `docker-compose.yml` - Main cluster configuration
- `docker-compose.master.yml` - Master + Worker 1 only  
- `docker-compose.worker.yml` - Additional worker node
- `test_cluster.sh` - Test script
