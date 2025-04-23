!pip install azure-eventhub fastavro

eventhub_namespace = 'iesstsabbadbaa-grp-06-10'
topic1_name = 'passenger_requests_10'
topic2_name = 'ride_status_10'
producer_connection_string = "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;SharedAccessKeyName=Producer_10;SharedAccessKey=zbHdilY9QSSJDgFtLf4hb3FnB/N5WVDQQ+AEhIHonHk="

from azure.eventhub import EventHubProducerClient, EventData
import fastavro
import json

# Helper to send AVRO records
def send_avro_records_to_eventhub(avro_file_path, eventhub_conn_str, eventhub_name):
    print(f"\n🔄 Sending records from {avro_file_path} to {eventhub_name}...")

    # Create EventHub Producer
    producer = EventHubProducerClient.from_connection_string(
        conn_str=eventhub_conn_str,
        eventhub_name=eventhub_name
    )

    # Read AVRO records
    with open(avro_file_path, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            try:
                event_data = EventData(json.dumps(record))
                producer.send_batch([event_data])
                print(f"✅ Sent record to {eventhub_name}")
            except Exception as e:
                print(f"❌ Error sending record: {e}")

    producer.close()
    print(f"✅ Done sending {eventhub_name}\n")

# Send both feeds
send_avro_records_to_eventhub("passenger_requests.avro", producer_connection_string, topic1_name)
send_avro_records_to_eventhub("ride_statuses.avro", producer_connection_string, topic2_name)
