import datetime
import requests
import json
import time
import subprocess
import threading
import schedule
import logging
import os
import signal
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    POLLING_ENDPOINT,
    POLLING_INTERVAL,
    SCHEDULER_LOG_FILE,
    LOG_LEVEL,
    DELAY
)
from logging.handlers import RotatingFileHandler


class CameraScheduler:
    def __init__(
        self,
        api_url,
        polling_interval=60,
        log_file="logs/scheduler.log",
        log_level="INFO",
        kafka_servers="localhost:9092",
    ):
        self.api_url = api_url
        self.polling_interval = polling_interval
        self.running = True
        self.active_processes = {}
        self.camera_schedules = {}
        self.anomaly_metadata = {}
        self.kafka_servers = kafka_servers

        self.setup_logger(log_file, log_level)

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def setup_logger(self, log_file, log_level):
        """Setup the logger"""
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        self.logger = logging.getLogger("camera_scheduler")

        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)

        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )

        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(
            f"Received shutdown signal {sig}, stopping all camera producers..."
        )
        self.running = False
        self.stop_all_cameras()

    def fetch_anomaly_data(self):
        """Poll the API for anomaly data"""
        try:
            self.logger.debug(f"Polling API at {self.api_url}")
            response = requests.get(self.api_url)            
            response.raise_for_status()
            data = response.json()
            self.logger.debug(f"API response received with {len(data.get('anomalies', []))} anomalies")
            return data
        except Exception as e:
            self.logger.error(f"Error fetching anomaly data: {e}")
            return None

    def should_camera_run_now(self, anomaly):
        """Determine if a camera should be running based on schedule"""
        today = datetime.datetime.now().strftime("%a").upper()
        
        days_check = not anomaly["daysOfWeek"] or today in anomaly["daysOfWeek"]
        
        if not days_check:
            self.logger.debug(f"Anomaly {anomaly['anomalyId']} not scheduled for today ({today})")
            return False
            
        if anomaly["startTime"] == "00:00:00" and anomaly["endTime"] == "00:00:00":
            self.logger.debug(f"Anomaly {anomaly['anomalyId']} scheduled 24/7")
            return True

        # Check if current time is within the schedule
        current_time = datetime.datetime.now().time()
        start_time = datetime.datetime.strptime(anomaly["startTime"], "%H:%M:%S").time()
        end_time = datetime.datetime.strptime(anomaly["endTime"], "%H:%M:%S").time()

        # Handle time ranges that cross midnight
        if start_time <= end_time:
            should_run = start_time <= current_time <= end_time
        else:  # Crosses midnight
            should_run = current_time >= start_time or current_time <= end_time
            
        self.logger.debug(
            f"Anomaly {anomaly['anomalyId']} time check: {start_time} <= {current_time} <= {end_time} = {should_run}"
        )
        return should_run

    def start_camera_producer(self, camera, organization_id, anomaly_id=None, topic_name="camera_feed", delay=1):
        """Start a camera producer process"""
        camera_id = camera["cameraId"]

        # Check if camera is already running
        if camera_id in self.active_processes and self.active_processes[camera_id].poll() is None:
            self.logger.info(f"Camera {camera_id} is already running")
            return

        log_file = f"logs/camera_{camera_id}.log"

        self.logger.info(
            f"Starting camera producer for camera {camera_id} at {camera['ipAddress']} for anomaly {anomaly_id}"
        )

        cmd = [
            "python",
            "producer.py",
            "--camera_url",
            camera["ipAddress"],
            "--organization_id",
            str(organization_id),
            "--topic_name",
            KAFKA_TOPIC,
            "--camera_id",
            str(camera_id),
            "--delay",
            str(DELAY),
            "--kafka_servers",
            self.kafka_servers,
            "--log_file",
            log_file,
            "--log_level",
            LOG_LEVEL,
        ]

        try:
            process = subprocess.Popen(cmd)
            self.active_processes[camera_id] = process
            self.logger.info(
                f"Started camera producer for camera {camera_id}, PID: {process.pid}"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to start camera producer for camera {camera_id}: {e}"
            )

    def stop_camera_producer(self, camera_id):
        """Stop a running camera producer"""
        if camera_id in self.active_processes:
            process = self.active_processes[camera_id]
            if process.poll() is None:
                self.logger.info(f"Stopping camera producer for camera {camera_id}")
                try:
                    process.terminate()
                    process.wait(timeout=5)
                    self.logger.info(f"Camera producer for camera {camera_id} stopped")
                except subprocess.TimeoutExpired:
                    self.logger.warning(
                        f"Camera producer for camera {camera_id} did not terminate gracefully, killing..."
                    )
                    process.kill()
                except Exception as e:
                    self.logger.error(
                        f"Error stopping camera producer for camera {camera_id}: {e}"
                    )
            del self.active_processes[camera_id]
        else:
            self.logger.debug(f"No active process found for camera {camera_id}")

    def stop_all_cameras(self):
        """Stop all running camera producers"""
        camera_ids = list(self.active_processes.keys())
        for camera_id in camera_ids:
            self.stop_camera_producer(camera_id)

    def extract_camera_info(self, data):
        """Extract camera information from anomaly data"""
        all_cameras = {}  # {camera_id: {camera_data}}
        camera_anomalies = {}  # {camera_id: [anomaly1, anomaly2, ...]}
        
        if not data or "anomalies" not in data:
            self.logger.error("Invalid data format or missing anomalies key")
            return all_cameras, camera_anomalies
            
        for anomaly in data["anomalies"]:
            # Skip anomalies without cameras
            if "cameras" not in anomaly or not anomaly["cameras"]:
                self.logger.debug(f"Anomaly {anomaly.get('anomalyId')} has no cameras")
                continue
                
            if "organization" not in anomaly:
                self.logger.debug(f"Anomaly {anomaly.get('anomalyId')} has no organization info")
                continue
                
            for camera in anomaly["cameras"]:
                if "cameraId" not in camera:
                    self.logger.warning(f"Camera in anomaly {anomaly.get('anomalyId')} missing cameraId")
                    continue
                    
                camera_id = camera["cameraId"]
                
                if camera_id not in all_cameras:
                    all_cameras[camera_id] = {
                        **camera,
                        "organization_id": anomaly["organization"]["id"]
                    }
                
                if camera_id not in camera_anomalies:
                    camera_anomalies[camera_id] = []
                    
                camera_anomalies[camera_id].append(anomaly)
                
        self.logger.info(f"Extracted {len(all_cameras)} cameras with {sum(len(anoms) for anoms in camera_anomalies.values())} anomaly associations")
        return all_cameras, camera_anomalies

    def process_anomaly_data(self, data):
        """Process anomaly data and schedule camera producers"""
        if not data:
            self.logger.warning("No data received from API")
            return

        all_cameras, camera_anomalies = self.extract_camera_info(data)
        
        for camera_id, camera_data in all_cameras.items():
            self.logger.debug(f"Camera {camera_id}: {camera_data.get('location')} - {camera_data.get('status')}")
            
        cameras_to_run = {}  # {camera_id: camera_data}
        
        for camera_id, camera_data in all_cameras.items():
            if camera_data.get("status") != "ONLINE":
                self.logger.debug(f"Camera {camera_id} is not ONLINE, status: {camera_data.get('status')}")
                continue
            
            should_run = False
            running_anomaly = None
            
            if camera_id in camera_anomalies:
                for anomaly in camera_anomalies[camera_id]:
                    if self.should_camera_run_now(anomaly):
                        should_run = True
                        running_anomaly = anomaly
                        break
            
            if should_run:
                self.logger.info(f"Camera {camera_id} should be running for anomaly {running_anomaly['anomalyId']}")
                cameras_to_run[camera_id] = {
                    "camera": camera_data,
                    "anomaly": running_anomaly,
                    "organization_id": camera_data["organization_id"]
                }
            else:
                self.logger.debug(f"Camera {camera_id} should not be running based on schedules")
        
        # Start cameras that should be running
        for camera_id, data in cameras_to_run.items():
            self.start_camera_producer(
                data["camera"], 
                data["organization_id"],
                data["anomaly"]["anomalyId"]
            )
            
        # Stop cameras that should not be running
        running_camera_ids = set(self.active_processes.keys())
        cameras_to_run_ids = set(cameras_to_run.keys())
        
        for camera_id in running_camera_ids - cameras_to_run_ids:
            self.logger.info(f"Camera {camera_id} should not be running, stopping...")
            self.stop_camera_producer(camera_id)
            
        # Log summary
        self.logger.info(f"Currently running cameras: {running_camera_ids}")
        self.logger.info(f"Cameras that should be running: {cameras_to_run_ids}")
        
        # Update metadata for change detection
        self.camera_schedules = camera_anomalies
        self.anomaly_metadata = {
            anomaly["anomalyId"]: anomaly["updatedAt"]
            for anomaly in data.get("anomalies", [])
        }

    def poll_and_process(self):
        """Poll the API and process the data"""
        if not self.running:
            return

        self.logger.debug("Polling for anomaly data...")
        data = self.fetch_anomaly_data()
        if data:
            self.process_anomaly_data(data)
        else:
            self.logger.error("Failed to fetch anomaly data")

    def start(self):
        """Start the scheduler"""
        self.logger.info("Starting camera scheduler")
        try:
            # Set up polling schedule
            schedule.every(self.polling_interval).seconds.do(self.poll_and_process)

            # Do an initial poll immediately
            self.poll_and_process()

            # Run the scheduler
            while self.running:
                schedule.run_pending()
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received, shutting down...")
        except Exception as e:
            self.logger.error(f"Error in scheduler: {e}")
        finally:
            self.logger.info("Stopping all camera producers...")
            self.stop_all_cameras()
            self.logger.info("Camera scheduler stopped")


if __name__ == "__main__":
    scheduler = CameraScheduler(
        api_url=POLLING_ENDPOINT,
        polling_interval=POLLING_INTERVAL,
        log_file=SCHEDULER_LOG_FILE,
        log_level=LOG_LEVEL,
        kafka_servers=KAFKA_BOOTSTRAP_SERVERS,
    )

    scheduler.start()