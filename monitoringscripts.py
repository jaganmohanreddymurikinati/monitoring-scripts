from google.cloud import monitoring_v3
from kafka import KafkaProducer
import time
import logging
import json
from google.protobuf.timestamp_pb2 import Timestamp  # used to handle Google Cloud timestamp format
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pymongo
import pytz

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MongoDB Atlas Connection Setup
def get_metrics_from_mongo(tenant_id, project_id):
    # Replace the MongoDB Atlas URI with your actual connection string
    client = pymongo.MongoClient("mongodb+srv://jagan:Jagan931@cluster0.u70we2h.mongodb.net/organizationDB?retryWrites=true&w=majority")
    db = client["organizationDB"]  # Replace with your MongoDB database name
    collection = db["organizations"]  # Replace with your MongoDB collection name

    # Query for the tenant and project
    record = collection.find_one({"organization_id": tenant_id, "project_id": project_id})
    if record:
        # Retrieve metrics and Kafka topics for each resource
        metrics = {}
        for resource in record["resources"]:
            for metric in resource["metrics"]:
                metrics[metric["metric_type"]] = metric["kafka_topic"]
        return metrics
    return {}

# Function to convert DatetimeWithNanoseconds to ISO format
def convert_timestamp_to_iso(timestamp):
    if isinstance(timestamp, datetime):
        # Ensure the timestamp is in UTC, if not you can use timestamp.astimezone(pytz.utc)
        timestamp_utc = timestamp.astimezone(pytz.utc)
        
        # Convert to IST by adding 5 hours and 30 minutes to UTC
        timestamp_ist = timestamp_utc + timedelta(hours=5, minutes=30)
        
        # Return the ISO formatted string
        return timestamp_ist.isoformat()
    
    return None

# Function to update metric status in MongoDB
def update_metric_status(tenant_id, project_id, metric_type, status, message=""):
    try:
        # MongoDB Atlas Connection Setup
        client = pymongo.MongoClient("mongodb+srv://jagan:Jagan931@cluster0.u70we2h.mongodb.net/organizationDB?retryWrites=true&w=majority")
        db = client["organizationDB"]  # Replace with your MongoDB database name
        collection = db["metric_status"]  # Define a new collection for metric status
        
        # Prepare the status document
        status_doc = {
            "metric_type": metric_type,
            "timestamp": datetime.utcnow().isoformat(),
            "status": status,
            "message": message,
            "tenant_id": tenant_id,
            "project_id": project_id
        }

        # Insert or update the document
        collection.update_one(
            {"metric_type": metric_type, "tenant_id": tenant_id, "project_id": project_id},
            {"$set": status_doc},
            upsert=True  # Insert a new document if it doesn't exist
        )

        logging.info(f"Updated status for metric {metric_type}: {status}")
    except Exception as e:
        logging.error(f"Error updating metric status in MongoDB: {e}")

class MonitoringClient:
    def __init__(self, project_id, tenant_id, kafka_bootstrap_servers="34.71.247.120:9092"):
        self.project_id = project_id
        self.tenant_id = tenant_id
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = f"projects/{self.project_id}"

        # Fetch metrics dynamically from MongoDB
        self.metrics = get_metrics_from_mongo(self.tenant_id, self.project_id)

        # Kafka Producer
        # This ensures that message sent to kafka is properly serialized
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def fetch_metric_data(self, metric_type, last_n_seconds=600):
        """Fetch metric data for the last 'n' seconds and send to Kafka."""
        try:
            # Set the initial status to 'in_progress'
            update_metric_status(self.tenant_id, self.project_id, metric_type, "in_progress", "Fetching data from Google Cloud Monitoring")
            
            if metric_type not in self.metrics:
                logging.error(f"Metric type {metric_type} not found in database configuration.")
                update_metric_status(self.tenant_id, self.project_id, metric_type, "error", "Metric type not found in database")
                return
            
            kafka_topic = self.metrics[metric_type]
            
            query = monitoring_v3.ListTimeSeriesRequest(
                name=self.project_name,
                filter=f'metric.type="{metric_type}"',
                interval={  # Define the time interval
                    "start_time": {"seconds": int(time.time() - last_n_seconds)},
                    "end_time": {"seconds": int(time.time())},
                },
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            )
            time_series_data = list(self.client.list_time_series(request=query))
            
            # Send each metric point to Kafka
            for time_series in time_series_data:
                for point in time_series.points:
                    timestamp = convert_timestamp_to_iso(point.interval.end_time)
                    metric_record = {
                        "metric_type": time_series.metric.type,
                        "timestamp": timestamp,
                        "value": point.value.double_value,
                        "tenant_id": self.tenant_id,
                        "project_id": self.project_id,
                    }
                    self.producer.send(kafka_topic, metric_record)  # Send data to Kafka
                    logging.info(f"Sent metric to Kafka: {metric_record}")

            # Update status to 'success'
            update_metric_status(self.tenant_id, self.project_id, metric_type, "success", "Metrics fetched and sent to Kafka")
            return time_series_data
        except Exception as e:
            # Update status to 'error' in case of any issues
            update_metric_status(self.tenant_id, self.project_id, metric_type, "error", str(e))
            logging.error(f"Error fetching data for {metric_type}: {e}")
            return []

# Function to run fetch operations concurrently using ThreadPoolExecutor
def fetch_metrics_concurrently(client, last_n_seconds=600):
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as per the need
        futures = []
        
        # Submit fetch tasks for each metric type concurrently
        for metric_type in client.metrics.keys():
            futures.append(executor.submit(client.fetch_metric_data, metric_type, last_n_seconds))
        
        
        # Wait for all futures to complete
        for future in futures:
            future.result()  # This will also raise exceptions if any occurred during execution

if __name__ == "__main__":
    project_id = "digital-splicer-448505-f3"
    tenant_id = "maplelabs"  # Dynamic tenant ID
    
    # Initialize Monitoring Client with Kafka
    monitoring_client = MonitoringClient(project_id, tenant_id)

    # Fetch and Send Metrics to Kafka concurrently (dynamically based on MongoDB)
    print("\n========== FETCHING METRICS ==========")
    fetch_metrics_concurrently(monitoring_client)
