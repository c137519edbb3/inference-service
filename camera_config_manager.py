# camera_config_manager.py
import argparse
import json
import sys
import os
from airflow.models import Variable
from airflow.settings import AIRFLOW_HOME

# Import update_camera_config function
try:
    sys.path.append(os.path.join(AIRFLOW_HOME, 'dags'))
    from airflow_camera_orchestration import update_camera_config
except ImportError:
    print("Error: Could not import from airflow_camera_orchestration.py")
    print(f"Ensure the script exists in {os.path.join(AIRFLOW_HOME, 'dags')}")
    sys.exit(1)

def list_cameras():
    """List all configured cameras"""
    try:
        camera_configs = Variable.get("camera_configs", deserialize_json=True, default=[])
        if not camera_configs:
            print("No cameras configured")
            return
        
        print(f"{'ID':<10} {'URL':<40} {'Schedule':<15} {'Topic':<15}")
        print("-" * 80)
        for config in camera_configs:
            print(f"{config['camera_id']:<10} {config.get('camera_url', 'N/A'):<40} {config.get('schedule', 'N/A'):<15} {config.get('topic_name', 'camera_feed'):<15}")
    except Exception as e:
        print(f"Error listing cameras: {e}")

def add_or_update_camera(args):
    """Add or update a camera configuration"""
    try:
        update_camera_config(
            camera_id=args.camera_id,
            camera_url=args.camera_url,
            schedule=args.schedule,
            topic_name=args.topic_name,
            kafka_servers=args.kafka_servers,
            timeout=args.timeout
        )
        print(f"Camera {args.camera_id} configuration updated successfully")
    except Exception as e:
        print(f"Error updating camera configuration: {e}")
        print(f"Airflow Home: {AIRFLOW_HOME}")

def delete_camera(camera_id):
    """Delete a camera configuration"""
    try:
        update_camera_config(camera_id=camera_id, delete=True)
        print(f"Camera {camera_id} removed successfully")
    except Exception as e:
        print(f"Error removing camera: {e}")

def main():
    parser = argparse.ArgumentParser(description='Manage camera configurations for Airflow orchestration')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all camera configurations')
    
    # Add/Update command
    add_parser = subparsers.add_parser('add', help='Add or update a camera configuration')
    add_parser.add_argument('--camera_id', required=True, help='Unique ID for this camera')
    add_parser.add_argument('--camera_url', help='URL of the IP camera')
    add_parser.add_argument('--schedule', help='Cron schedule expression (e.g. "*/30 * * * *" for every 30 minutes)')
    add_parser.add_argument('--topic_name', help='Kafka topic name to publish frames')
    add_parser.add_argument('--kafka_servers', help='Kafka bootstrap servers')
    add_parser.add_argument('--timeout', type=int, help='Timeout in seconds for camera process execution')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete a camera configuration')
    delete_parser.add_argument('--camera_id', required=True, help='ID of camera to delete')
    
    args = parser.parse_args()
    
    print(f"Using Airflow Home: {AIRFLOW_HOME}")
    
    if args.command == 'list':
        list_cameras()
    elif args.command == 'add':
        add_or_update_camera(args)
    elif args.command == 'delete':
        delete_camera(args.camera_id)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()