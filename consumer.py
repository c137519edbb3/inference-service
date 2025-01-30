# Kafka Installation Reference: https://www.youtube.com/watch?v=BwYFuhVhshI

import base64
import json
from kafka import KafkaConsumer
import cv2
import numpy as np
from models import CLIPInferenceModel

class MLConsumer:
    def __init__(self, topic_name, model_class=CLIPInferenceModel, model_kwargs={}, kafka_bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Load pre-trained model
        self.model = model_class(**model_kwargs)
        
    def set_labels(self, labels):
        self.model.set_labels(labels)
        
    def process_stream(self):
        try:
            print("Starting to process stream...")
            for message in self.consumer:
                frame_data = message.value['frame']
                
                # Run inference
                predictions = self.model.predict(frame_data)
                
                # Print results
                print(f"\nTimestamp: {message.value['timestamp']}")
                print("Predictions:")
                for label, confidence in predictions:
                    print(f"{label}: {confidence*100:.2f}%")
                print("-------------------")
                
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()


if __name__ == "__main__":

    custom_labels = [
        "a photo of a student getting punished",
        "a photo of a teacher using mobile phone",
        "a photo of a teacher checking notebook",
        "a photo of students fighting"
    ]
    
    consumer = MLConsumer(
        topic_name='camera_feed',
        model_kwargs={'model_name': 'openai/clip-vit-large-patch14'}
    )
    consumer.set_labels(custom_labels)

    consumer.process_stream()