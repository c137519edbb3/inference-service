import datetime
import random
import cv2
import json
import numpy as np
from kafka import KafkaProducer
from time import sleep
import base64

class CameraProducer:
    def __init__(self, camera_url, topic_name, kafka_bootstrap_servers='localhost:9092'):
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Connect to IP camera
        self.camera = cv2.VideoCapture(camera_url)
        if not self.camera.isOpened():
            raise Exception(f"Failed to connect to IP camera at {camera_url}")
        print(f"Successfully connected to IP camera at {camera_url}")
        
    def start_streaming(self):
        try:
            while True:
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
                    'timestamp': str(datetime.datetime.now())
                })
                # print('Frame sent to Kafka Broker')
                sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping stream...")
        except Exception as e:
            print(f"Error during streaming: {e}")
        finally:
            print("Closing camera connection...")
            self.camera.release()
            self.producer.close()



if __name__ == "__main__":

    camera_url = "https://192.168.100.9:8080/video"

    producer = CameraProducer(topic_name='camera_feed', camera_url=camera_url)
    producer.start_streaming()