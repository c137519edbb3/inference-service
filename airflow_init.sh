#!/bin/bash
# airflow_init.sh - Initialize and start Airflow for camera orchestration

# Exit on error
set -e

# Get current project directory
PROJECT_DIR=$(pwd)
echo "Using project directory: $PROJECT_DIR"

# Configuration
AIRFLOW_HOME=${PROJECT_DIR}/airflow
LOG_DIR=${PROJECT_DIR}/logs/airflow
DAG_DIR=${AIRFLOW_HOME}/dags
PRODUCER_SCRIPT_PATH=${PROJECT_DIR}/producer.py

# Create necessary directories
echo "Creating directories..."
mkdir -p ${AIRFLOW_HOME}
mkdir -p ${LOG_DIR}
mkdir -p ${DAG_DIR}

# Check if producer script exists
if [ ! -f "$PRODUCER_SCRIPT_PATH" ]; then
    echo "Error: Producer script not found at $PRODUCER_SCRIPT_PATH"
    exit 1
fi

# Copy scripts to Airflow DAG directory
echo "Copying scripts to Airflow DAG directory..."
cp -v ${PROJECT_DIR}/airflow_camera_orchestration.py ${DAG_DIR}/
cp -v ${PROJECT_DIR}/camera_config_manager.py ${DAG_DIR}/
cp -v ${PRODUCER_SCRIPT_PATH} ${DAG_DIR}/

# Set AIRFLOW_HOME environment variable
export AIRFLOW_HOME=${AIRFLOW_HOME}

# Initialize Airflow database if it doesn't exist
if [ ! -f "${AIRFLOW_HOME}/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init
fi

# Create Airflow user if it doesn't exist
airflow users list | grep -q "admin" || \
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com

# Create airflow.cfg if it doesn't exist
if [ ! -f "${AIRFLOW_HOME}/airflow.cfg" ]; then
    echo "Warning: airflow.cfg not found. Airflow may need to be configured."
fi

# Start Airflow services in background
echo "Starting Airflow scheduler..."
nohup airflow scheduler > ${LOG_DIR}/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "Airflow scheduler started with PID: $SCHEDULER_PID"

# Optional: Start webserver if needed
if [ "$1" == "--with-webserver" ]; then
    echo "Starting Airflow webserver..."
    nohup airflow webserver -p 8080 > ${LOG_DIR}/webserver.log 2>&1 &
    WEBSERVER_PID=$!
    echo "Airflow webserver started with PID: $WEBSERVER_PID"
fi

echo "Airflow services started successfully"
echo "Use camera_config_manager.py to manage camera configurations"
echo ""
echo "Example commands:"
echo "  AIRFLOW_HOME=${AIRFLOW_HOME} python ${DAG_DIR}/camera_config_manager.py list"
echo "  AIRFLOW_HOME=${AIRFLOW_HOME} python ${DAG_DIR}/camera_config_manager.py add --camera_id cam1 --camera_url http://example.com/camera1 --schedule '*/30 * * * *'"
echo "  AIRFLOW_HOME=${AIRFLOW_HOME} python ${DAG_DIR}/camera_config_manager.py delete --camera_id cam1"
echo ""
echo "Log directory: ${LOG_DIR}"