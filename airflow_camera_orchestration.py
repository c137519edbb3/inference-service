# airflow_camera_orchestration.py
import os
import json
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'airflow', 'camera_orchestration.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Path to the producer script
PRODUCER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'producer.py')

# Default Kafka server
DEFAULT_KAFKA_SERVERS = 'localhost:9092'
DEFAULT_TOPIC_NAME = 'camera_feed'

# Make sure the producer script exists
if not os.path.exists(PRODUCER_SCRIPT):
    raise FileNotFoundError(f"Producer script not found at {PRODUCER_SCRIPT}")

def get_camera_configs():
    """
    Get camera configurations from Airflow Variables.
    If not set, initialize with default values.
    """
    try:
        camera_configs = Variable.get("camera_configs", deserialize_json=True)
        if not camera_configs:
            # Initialize with empty list if not set
            camera_configs = []
            Variable.set("camera_configs", json.dumps(camera_configs))
    except Exception as e:
        logger.error(f"Error getting camera configs: {e}")
        camera_configs = []
        Variable.set("camera_configs", json.dumps(camera_configs))
    
    return camera_configs

def run_camera_producer(camera_id, camera_url, topic_name=DEFAULT_TOPIC_NAME, kafka_servers=DEFAULT_KAFKA_SERVERS, timeout=3600):
    """
    Run the camera producer script as a subprocess with a timeout.
    """
    logger.info(f"Starting camera producer for camera_id: {camera_id}, url: {camera_url}")
    
    try:
        cmd = [
            'python', PRODUCER_SCRIPT,
            '--camera_url', camera_url,
            '--topic_name', topic_name,
            '--camera_id', str(camera_id),
            '--kafka_servers', kafka_servers
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for the process to complete or timeout
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            if process.returncode != 0:
                logger.error(f"Camera producer failed for camera_id {camera_id}: {stderr}")
            else:
                logger.info(f"Camera producer completed successfully for camera_id {camera_id}")
                
        except subprocess.TimeoutExpired:
            process.kill()
            logger.info(f"Camera producer timed out and was terminated for camera_id {camera_id}")
            process.communicate()  # Clean up
            
    except Exception as e:
        logger.error(f"Error running camera producer for camera_id {camera_id}: {e}")

def create_camera_task(dag, camera_config):
    """
    Create a task for a specific camera configuration.
    """
    camera_id = camera_config['camera_id']
    camera_url = camera_config['camera_url']
    topic_name = camera_config.get('topic_name', DEFAULT_TOPIC_NAME)
    kafka_servers = camera_config.get('kafka_servers', DEFAULT_KAFKA_SERVERS)
    timeout = camera_config.get('timeout', 3600)  # Default 1 hour timeout
    
    return PythonOperator(
        task_id=f'camera_producer_{camera_id}',
        python_callable=run_camera_producer,
        op_kwargs={
            'camera_id': camera_id,
            'camera_url': camera_url,
            'topic_name': topic_name,
            'kafka_servers': kafka_servers,
            'timeout': timeout
        },
        dag=dag
    )

def create_dynamic_dags():
    """
    Create dynamic DAGs based on camera configurations.
    This allows runtime modification of schedules.
    """
    camera_configs = get_camera_configs()
    
    for camera_config in camera_configs:
        camera_id = camera_config['camera_id']
        schedule = camera_config.get('schedule', '0 * * * *')  # Default: hourly
        
        dag_id = f'camera_producer_dag_{camera_id}'
        
        # Create the DAG
        dag = DAG(
            dag_id=dag_id,
            default_args={
                'owner': 'airflow',
                'depends_on_past': False,
                'start_date': days_ago(1),
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
            },
            schedule_interval=schedule,
            catchup=False,
            is_paused_upon_creation=False
        )
        
        # Create task
        task = create_camera_task(dag, camera_config)
        
        # Register the DAG globally
        globals()[dag_id] = dag
        logger.info(f"Created DAG {dag_id} with schedule {schedule}")

# Create a DAG to check for configuration updates
config_update_dag = DAG(
    'camera_config_update',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(minutes=5),  # Check for updates every 5 minutes
    catchup=False,
)

# This function provides an API to update camera configurations
def update_camera_config(camera_id, camera_url=None, schedule=None, topic_name=None, 
                         kafka_servers=None, timeout=None, delete=False):
    """
    Update the configuration for a specific camera.
    Can be called externally to modify schedules at runtime.
    """
    camera_configs = get_camera_configs()
    
    if delete:
        # Remove the camera if delete flag is set
        camera_configs = [c for c in camera_configs if c['camera_id'] != camera_id]
        logger.info(f"Removed camera {camera_id} from configurations")
    else:
        # Find existing config or create new
        camera_config = next((c for c in camera_configs if c['camera_id'] == camera_id), None)
        
        if camera_config is None:
            if camera_url is None:
                raise ValueError(f"Camera URL is required when adding a new camera")
                
            # Create new config
            camera_config = {'camera_id': camera_id}
            camera_configs.append(camera_config)
            logger.info(f"Added new camera {camera_id} to configurations")
        else:
            logger.info(f"Updating configuration for camera {camera_id}")
        
        # Update fields if provided
        if camera_url is not None:
            camera_config['camera_url'] = camera_url
        if schedule is not None:
            camera_config['schedule'] = schedule
        if topic_name is not None:
            camera_config['topic_name'] = topic_name
        if kafka_servers is not None:
            camera_config['kafka_servers'] = kafka_servers
        if timeout is not None:
            camera_config['timeout'] = timeout
    
    # Save updated configurations
    Variable.set("camera_configs", json.dumps(camera_configs))
    logger.info("Camera configurations updated successfully")
    
    # For debugging
    return camera_configs

def check_and_update_dags():
    """
    Check for configuration updates and reload DAGs if needed.
    This is a placeholder since Airflow doesn't support truly dynamic DAG reloading.
    Instead, we use a file-based approach.
    """
    logger.info("Checking for camera configuration updates")
    
    # This task doesn't need to do much - the DAGs will be reloaded
    # when Airflow's scheduler performs its regular DAG bag refresh
    
    # In a real implementation, you might want to:
    # 1. Compare current DAGs with configurations
    # 2. Write new DAG files to the DAGs folder if needed
    # 3. Signal Airflow to reload DAGs

# Create task to check for configuration updates
check_config_task = PythonOperator(
    task_id='check_and_update_camera_configs',
    python_callable=check_and_update_dags,
    dag=config_update_dag,
)

# Helper script to add/update camera configurations
def add_camera_example():
    """
    Example of how to add or update a camera configuration.
    This function can be used in an external script to manage camera schedules.
    """
    update_camera_config(
        camera_id="cam1",
        camera_url="https://192.168.100.9:8080/video",
        schedule="*/30 * * * *",  # Run every 30 minutes
        topic_name="camera_feed",
        kafka_servers="localhost:9092",
        timeout=1800  # 30 minutes
    )

# Initialize DAGs
create_dynamic_dags()