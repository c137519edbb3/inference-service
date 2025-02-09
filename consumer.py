import time
import json
import firebase_admin
from kafka import KafkaConsumer
from firebase_admin import credentials, firestore 
from firebase_admin import credentials
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
from models import CLIPInferenceModel

class MLConsumer:
    def __init__(self, topic_name, good_classes, bad_classes, model_class=CLIPInferenceModel, model_kwargs={}, kafka_bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )


        cred = credentials.Certificate("./key.json")
        firebase_admin.initialize_app(cred)

        self.db = firestore.client()
        self.db_ref = self.db.collection("organizations")

        self.model = model_class(**model_kwargs)

        # Thread pool for batch writes
        self.executor = ThreadPoolExecutor(max_workers=5)

        # Log queue for batch writes
        self.log_queue = Queue()

        self.BATCH_SIZE = 3

        self.good_classes = set(good_classes)
        self.bad_classes = set(bad_classes)

        # Background writer thread
        self.writer_thread = threading.Thread(target=self.batch_write_logs, daemon=True)
        self.writer_thread.start()

    def set_labels(self, labels):
        self.model.set_labels(labels)

    def enqueue_log(self, organization_id, camera_id, log_data):
        """Queue log for batch writing"""
        self.log_queue.put((organization_id, camera_id, log_data))
        
    def batch_write_logs(self):
        """Background thread: Batch Firestore writes for efficiency"""
        while True:
            batch_data = []

            while not self.log_queue.empty() and len(batch_data) < self.BATCH_SIZE:
                organization_id, camera_id, log_data = self.log_queue.get()
                batch_data.append((organization_id, camera_id, log_data))

            if batch_data:
                batch = self.db.batch()

                for org_id, cam_id, log_data in batch_data:
                    # Push log data to Firestore
                    log_ref = (
                        self.db_ref.document(org_id)
                        .collection("cameras")
                        .document(cam_id)
                        .collection("logs")
                        .document()  # Auto-generate ID
                    )
                    batch.set(log_ref, log_data)
                    batch.commit()
                    print(f"🔥 Log pushed to Firestore with ID: {log_ref.id}")

                print(f"🔥 Batch committed with {len(batch_data)} logs.")

            time.sleep(1)

    def process_stream(self):
        try:
            print("Starting to process stream...")
            for message in self.consumer:
                frame_data = message.value['frame']
                organization_id = message.value['organization_id']
                camera_id = message.value['camera_id']
                timestamp = message.value['timestamp']
                
                predictions = self.model.predict(frame_data)

                best_label, best_confidence = max(predictions, key=lambda x: x[1])
                
                if best_label in self.bad_classes:
                    category = "BAD CLASS ⚠️"

                    log_entry = {
                        "timestamp": timestamp,
                        "event": best_label,
                        "confidence": best_confidence,
                        "camera_id": camera_id,
                        "organization_id": organization_id,
                    }
                    self.enqueue_log(organization_id, camera_id, log_entry)

                    print(f"\n🚨 BAD CLASS DETECTED! Queued for Firestore.")
                    
                elif best_label in self.good_classes:
                    category = "GOOD CLASS ✅"
                else:
                    category = "UNKNOWN"

                print(f"\nTimestamp: {message.value['timestamp']}")
                print(f"{best_label} ({best_confidence*100:.2f}%) - {category}")
                print("-------------------")
                
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    good_classes = [
        "a photo of person sitting on chair",
        "a photo of person walking through door",
        "a photo of person looking around room",
        "a photo of person opening door"
    ]
    
    bad_classes = [
        "a photo of person fighting",
    ]
    
    all_labels = good_classes + bad_classes
    
    consumer = MLConsumer(
        topic_name='camera_feed',
        good_classes=good_classes,
        bad_classes=bad_classes,
        model_kwargs={'model_name': 'openai/clip-vit-large-patch14'}
    )
    
    consumer.set_labels(all_labels)
    consumer.process_stream()
