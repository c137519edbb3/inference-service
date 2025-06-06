SERVER_URL = "https://eyeconai-api.vercel.app"
POLLING_ENDPOINT = f"{SERVER_URL}/api/public/anomalies"
POLLING_INTERVAL = 60 # seconds
SCHEDULER_LOG_FILE = "scheduler.log"
LOG_LEVEL = "DEBUG"
KAFKA_BOOTSTRAP_SERVERS = "34.31.160.13:9092"
KAFKA_TOPIC = "camera_feed"
KAFKA_GROUP_ID = "camera_producer_group"
DELAY = 1  # seconds