#!/bin/bash

# Set Airflow username and password
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=admin

# Check if Docker is installed
if ! command -v docker &> /dev/null
then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null
then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Pull the latest image or rebuild the Docker images to reflect changes
echo "Pulling the latest Docker images (if applicable) or rebuilding the images..."
docker-compose pull
docker-compose build --no-cache

# Stop and remove the existing containers if running
echo "Stopping and removing existing Airflow containers..."
docker-compose down

# Initialize the Airflow database using Docker Compose
echo "Initializing the Airflow database..."
docker-compose up airflow-init

# Run airflow db init to initialize the metadata database
echo "Running airflow db init..."
docker-compose run --rm webserver airflow db init

# Create an Airflow user
echo "Creating Airflow user..."
docker-compose run --rm webserver airflow users create \
    --username $AIRFLOW_USERNAME \
    --password $AIRFLOW_PASSWORD \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start the Airflow services
echo "Starting Airflow services..."
docker-compose up -d

# Provide feedback to the user
echo "Airflow is starting up. You can access the web UI at http://localhost:8080"
echo "Login with username: $AIRFLOW_USERNAME and password: $AIRFLOW_PASSWORD"

# Optionally, you can add a command to follow the logs
# Uncomment the following line if you want to see the logs
# docker-compose logs -f
