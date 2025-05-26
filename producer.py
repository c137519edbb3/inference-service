import datetime
import cv2
import json
import numpy as np
from kafka import KafkaProducer
from time import sleep
import base64
import argparse
import signal
import sys
import logging
import os

from logging.handlers import RotatingFileHandler


class CameraProducer:
    def __init__(
        self,
        camera_url,
        organization_id,
        topic_name,
        camera_id,
        delay,
        log_file,
        log_level="INFO",
        kafka_bootstrap_servers="localhost:9092",
    ):
        self.topic_name = topic_name
        self.camera_id = int(camera_id)
        self.running = True
        self.organization_id = int(organization_id)
        self.delay = int(delay)

        # Setup logging
        self.setup_logger(log_file, log_level)

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Connect to Kafka
        self.logger.info(f"Connecting to Kafka at {kafka_bootstrap_servers}")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            self.logger.info("Successfully connected to Kafka")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise

        # Connect to IP camera
        self.logger.info(f"Connecting to IP camera at {camera_url}")
        self.camera = cv2.VideoCapture(camera_url)
        if not self.camera.isOpened():
            self.logger.error(f"Failed to connect to IP camera at {camera_url}")
            raise Exception(f"Failed to connect to IP camera at {camera_url}")
        self.logger.info(f"Successfully connected to IP camera at {camera_url}")

    def setup_logger(self, log_file, log_level):
        """Setup the logger"""
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set up logger
        self.logger = logging.getLogger(f"camera_producer_{self.camera_id}")

        # Set log level
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)

        # Create rotating file handler (10MB per file, max 5 files)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )

        # Create console handler
        console_handler = logging.StreamHandler()

        # Create formatter and add it to the handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received shutdown signal {sig}, closing resources...")
        self.running = False

    def start_streaming(self):
        try:
            self.logger.info(f"Starting camera stream for camera {self.camera_id}")
            while self.running:
                success, frame = self.camera.read()
                if not success:
                    self.logger.warning("Failed to capture frame from IP camera")
                    sleep(self.delay)
                    continue

                # Convert frame to base64 string for sending via Kafka
                _, buffer = cv2.imencode(".jpg", frame)
                frame_base64 = base64.b64encode(buffer).decode("utf-8")

                # Send frame to Kafka
                self.producer.send(
                    self.topic_name,
                    {
                        "frame": frame_base64,
                        "timestamp": str(datetime.datetime.now().timestamp()),
                        "organization_id": self.organization_id,
                        "camera_id": self.camera_id,
                    },
                )
                self.logger.debug(
                    f"Frame from camera {self.camera_id} sent to Kafka topic {self.topic_name}"
                )
                sleep(self.delay)

        except KeyboardInterrupt:
            self.logger.info("Stopping stream due to keyboard interrupt...")
        except Exception as e:
            self.logger.error(f"Error during streaming: {e}")
            raise
        finally:
            self.logger.info("Closing camera connection...")
            self.camera.release()
            self.producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Camera Producer for Kafka")
    parser.add_argument("--camera_url", required=True, help="URL of the IP camera")
    parser.add_argument(
        "--organization_id", required=True, help="ID of the organization"
    )
    parser.add_argument(
        "--topic_name", default="camera_feed", help="Kafka topic name to publish frames"
    )
    parser.add_argument("--camera_id", required=True, help="Unique ID for this camera")
    parser.add_argument(
        "--delay", required=True, help="Delay between frames in seconds"
    )
    parser.add_argument(
        "--kafka_servers", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument("--log_file", required=True, help="Path to the log file")
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    args = parser.parse_args()

    producer = CameraProducer(
        camera_url=args.camera_url,
        organization_id=args.organization_id,
        topic_name=args.topic_name,
        camera_id=args.camera_id,
        delay=args.delay,
        log_file=args.log_file,
        log_level=args.log_level,
        kafka_bootstrap_servers=args.kafka_servers,
    )
    producer.start_streaming()


#     camera_url = "https://192.168.100.9:8080/video"

#     producer = CameraProducer(topic_name='camera_feed', camera_url=camera_url)
#     producer.start_streaming()

# python producer.py --camera_url "https://192.168.100.9:8080/video" --organization_id "1" --camera_id "1" --delay 1 --log_file "/camera_logs.log" --log_level INFO
