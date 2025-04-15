# airflow_init.ps1 - Initialize and start Airflow for camera orchestration on Windows

# Get current project directory
$PROJECT_DIR = Get-Location
Write-Host "Using project directory: $PROJECT_DIR"

# Configuration
$AIRFLOW_HOME = "$PROJECT_DIR\airflow"
$LOG_DIR = "$PROJECT_DIR\logs\airflow"
$DAG_DIR = "$AIRFLOW_HOME\dags"
$PRODUCER_SCRIPT_PATH = "$PROJECT_DIR\producer.py"

# Create necessary directories
Write-Host "Creating directories..."
New-Item -ItemType Directory -Force -Path $AIRFLOW_HOME | Out-Null
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null
New-Item -ItemType Directory -Force -Path $DAG_DIR | Out-Null

# Check if producer script exists
if (-Not (Test-Path $PRODUCER_SCRIPT_PATH)) {
    Write-Host "Error: Producer script not found at $PRODUCER_SCRIPT_PATH"
    exit 1
}

# Copy scripts to Airflow DAG directory
Write-Host "Copying scripts to Airflow DAG directory..."
Copy-Item -Path "$PROJECT_DIR\airflow_camera_orchestration.py" -Destination $DAG_DIR -Force
Copy-Item -Path "$PROJECT_DIR\camera_config_manager.py" -Destination $DAG_DIR -Force
Copy-Item -Path $PRODUCER_SCRIPT_PATH -Destination $DAG_DIR -Force

# Set AIRFLOW_HOME environment variable
$env:AIRFLOW_HOME = $AIRFLOW_HOME

# Initialize Airflow database if it doesn't exist
if (-Not (Test-Path "$AIRFLOW_HOME\airflow.db")) {
    Write-Host "Initializing Airflow database..."
    airflow db init
}

# Create Airflow user if it doesn't exist
$userExists = airflow users list | Select-String "admin"
if (-Not $userExists) {
    airflow users create `
        --username admin `
        --password admin `
        --firstname Admin `
        --lastname User `
        --role Admin `
        --email admin@example.com
}

# Create airflow.cfg if it doesn't exist
if (-Not (Test-Path "$AIRFLOW_HOME\airflow.cfg")) {
    Write-Host "Warning: airflow.cfg not found. Airflow may need to be configured."
}

# Start Airflow services in background
Write-Host "Starting Airflow scheduler..."
Start-Process -FilePath "airflow" -ArgumentList "scheduler" -RedirectStandardOutput "$LOG_DIR\scheduler.log" -RedirectStandardError "$LOG_DIR\scheduler_error.log" -NoNewWindow
Write-Host "Airflow scheduler started"

# Optional: Start webserver if needed
if ($args -contains "--with-webserver") {
    Write-Host "Starting Airflow webserver..."
    Start-Process -FilePath "airflow" -ArgumentList "webserver", "-p", "8080" -RedirectStandardOutput "$LOG_DIR\webserver.log" -RedirectStandardError "$LOG_DIR\webserver_error.log" -NoNewWindow
    Write-Host "Airflow webserver started"
}

Write-Host "Airflow services started successfully"
Write-Host "Use camera_config_manager.py to manage camera configurations"
Write-Host ""
Write-Host "Example commands:"
Write-Host "  $env:AIRFLOW_HOME='$AIRFLOW_HOME'; python $DAG_DIR\camera_config_manager.py list"
Write-Host "  $env:AIRFLOW_HOME='$AIRFLOW_HOME'; python $DAG_DIR\camera_config_manager.py add --camera_id cam1 --camera_url http://example.com/camera1 --schedule '*/30 * * * *'"
Write-Host "  $env:AIRFLOW_HOME='$AIRFLOW_HOME'; python $DAG_DIR\camera_config_manager.py delete --camera_id cam1"
Write-Host ""
Write-Host "Log directory: $LOG_DIR"