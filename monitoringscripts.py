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
    # MongoDB Atlas URI with our actual connection string
    client = pymongo.MongoClient("mongodb+srv://jagan:Jagan931@cluster0.u70we2h.mongodb.net/organizationDB?retryWrites=true&w=majority")
    db = client["organizationDB"]  # MongoDB database name
    collection = db["organizations"]  # MongoDB collection name

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
        # Ensure the timestamp is in UTC,use timestamp.astimezone(pytz.utc)
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
        db = client["organizationDB"]  #your MongoDB database name
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
            upsert=True  # Inserting a new document if it doesn't exist
        )

        logging.info(f"Updated status for metric {metric_type}: {status}")
    except Exception as e:
        logging.error(f"Error updating metric status in MongoDB: {e}")

class MonitoringClient:
    def __init__(self, project_id, tenant_id, kafka_bootstrap_servers="kafka:9092"):
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

# MongoDB Schedule Fetch and Check
def get_schedules_from_mongo():
    try:
        # MongoDB Atlas Connection Setup
        client = pymongo.MongoClient("mongodb+srv://jagan:Jagan931@cluster0.u70we2h.mongodb.net/organizationDB?retryWrites=true&w=majority")
        db = client["organizationDB"]
        collection = db["schedules"]

        # Query to get schedules
        schedules = collection.find()
        return schedules
    except Exception as e:
        logging.error(f"Error fetching schedules: {e}")
        return []

def is_schedule_lapsed(schedule):
    # Get the current time in UTC
    current_time = datetime.utcnow()
    
    # Get the scheduled time
    schedule_time = schedule["schedule_time"]
    schedule_interval = schedule["schedule_interval"]

    # Calculate the next execution time by adding the interval to the schedule time
    next_execution_time = schedule_time + timedelta(seconds=schedule_interval)
    
     # Check if the current time is greater than or equal to the next scheduled time
    if current_time >= next_execution_time:
        return True
    return False

def trigger_metric_fetch(project_id, tenant_id):
    try:
        # Initialize your MonitoringClient class
        monitoring_client = MonitoringClient(project_id, tenant_id)

        # Fetch and send metrics to Kafka
        fetch_metrics_concurrently(monitoring_client)
        logging.info(f"Metrics fetched for project: {project_id}")
    except Exception as e:
        logging.error(f"Error triggering metric fetch for project {project_id}: {e}")

def scheduler():
    schedules = get_schedules_from_mongo()
    
    for schedule in schedules:
        if is_schedule_lapsed(schedule):
            # If the schedule has lapsed, trigger the metric fetch
            logging.info(f"Schedule for project {schedule['project_id']} has lapsed. Fetching metrics...")
            trigger_metric_fetch(schedule['project_id'], schedule['organization_id'])
	    # After execution, update the schedule_time in DB to the current time
            client = pymongo.MongoClient("mongodb+srv://jagan:Jagan931@cluster0.u70we2h.mongodb.net/organizationDB")
            db = client["organizationDB"]
            collection = db["schedules"]
            collection.update_one(
                {"_id": schedule["_id"]},
                {"$set": {"schedule_time": datetime.utcnow()}}
            )
        else:
            logging.info(f"Schedule for project {schedule['project_id']} has not lapsed yet.")


if __name__ == "__main__":
    while True:
        # Run the scheduler every minute (or as needed)
        scheduler()
        time.sleep(60)  # Sleep for 60 seconds
