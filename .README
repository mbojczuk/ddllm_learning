# Docker Commands

## Starting the Containers

To start both containers:
```bash
docker-compose up -d

To start individual containers (if they already exist):
```bash
docker start trino_postgres_postgres_1
docker start trino_postgres_trino_1

## Stopping the Containers
To stop both containers and remove networks:
```bash
docker-compose down

Or to stop them individually:
```bash
docker stop trino_postgres_postgres_1
docker stop trino_postgres_trino_1

## Accessing Containers

To open a shell inside a container:

### PostgreSQL Container

```bash
docker exec -it trino_postgres_postgres_1 bash

### Trino Container

```bash
docker exec -it trino_postgres_trino_1 bash

## Checking Container Status
To see the status of the containers:
```bash
docker ps -a