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

class CameraProducer:
    def __init__(self, camera_url, topic_name, camera_id, kafka_bootstrap_servers='localhost:9092'):
        self.topic_name = topic_name
        self.camera_id = camera_id
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Connect to Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Connect to IP camera
        self.camera = cv2.VideoCapture(camera_url)
        if not self.camera.isOpened():
            raise Exception(f"Failed to connect to IP camera at {camera_url}")
        print(f"Successfully connected to IP camera at {camera_url}")
    
    def signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        print(f"Received shutdown signal {sig}, closing resources...")
        self.running = False
        
    def start_streaming(self):
        try:
            while self.running:
                success, frame = self.camera.read()
                if not success:
                    print("Failed to capture frame from IP camera")
                    sleep(1)
                    continue
                
                # Convert frame to base64 string for sending via Kafka
                _, buffer = cv2.imencode('.jpg', frame)
                frame_base64 = base64.b64encode(buffer).decode('utf-8')
                
                # Send frame to Kafka
                self.producer.send(self.topic_name, {
                    'frame': frame_base64,
                    'timestamp': str(datetime.datetime.now().timestamp()),
                    'organization_id': 1,
                    'camera_id': self.camera_id,
                })
                print(f'Frame from camera {self.camera_id} sent to Kafka topic {self.topic_name}')
                sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping stream...")
        except Exception as e:
            print(f"Error during streaming: {e}")
            raise
        finally:
            print("Closing camera connection...")
            self.camera.release()
            self.producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Camera Producer for Kafka')
    parser.add_argument('--camera_url', required=True, help='URL of the IP camera')
    parser.add_argument('--topic_name', default='camera_feed', help='Kafka topic name to publish frames')
    parser.add_argument('--camera_id', required=True, help='Unique ID for this camera')
    parser.add_argument('--kafka_servers', default='localhost:9092', help='Kafka bootstrap servers')
    
    args = parser.parse_args()

    producer = CameraProducer(
        camera_url=args.camera_url,
        topic_name=args.topic_name,
        camera_id=args.camera_id,
        kafka_bootstrap_servers=args.kafka_servers
    )
    producer.start_streaming()