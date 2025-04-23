# Milestone-1 (Ride-Hailing Data Simulation Project)

## Introduction
This project simulates realistic data for a ride-hailing service, generating synthetic ride requests and status updates. The goal is to create datasets that mimic real-world scenarios, including dynamic pricing, traffic/weather impacts, and user/driver behavior patterns. The data is serialized into AVRO format for efficient storage and stream processing.

## Project Structure
### Milestone 1: Data Feed Generation
1. **Two Data Feeds**:
   - **Passenger Requests**: Captures ride requests/cancellations, locations, fares, and user metadata.
   - **Ride Statuses**: Tracks ride lifecycle (accepted, ongoing, completed, cancelled) with driver/vehicle details.

2. **AVRO Schemas**:
   - **PassengerRequest Schema**: 
     - Fields: `request_id`, `user_id`, `event_type`, `pickup/dropoff` coordinates, `fare_estimate`, `ride_purpose`, etc.
     - Enums: `EventType`, `PaymentMethod`, `VehicleType`, `RidePurpose`.
   - **RideStatus Schema**:
     - Fields: `ride_id`, `status`, `driver_id`, `traffic_condition`, `surge_multiplier`, `payment_status`, etc.
     - Enums: `RideStatusEnum`, `CancellationReason`, `TrafficCondition`.

### Key Features
- **Realism**:
  - **Peak Hours**: Higher demand during weekday rush hours (7–10:30 AM, 4–8 PM) and weekend nights.
  - **Dynamic Pricing**: Surge multipliers based on time, weather, and traffic.
  - **Geospatial Data**: Realistic pickup/dropoff addresses using `mimesis`.
- **Scalability**: Generates 1,000+ records with configurable parameters.
- **Configurability**: Adjust `active_drivers`, `demand_level`, and output directories.

---

## Installation
1. **Dependencies**:
   ```bash
   pip install Faker mimesis fastavro
2. **Libraries Used**:
   - `Faker` and `mimesis for data generation
   - `fastavro` for AVRO serialization
   - `numpy`, `pandas` for data manipulation
  
## Usage
### Generate Data
Run the Jupyter notebook to:
1. Create synthetic ride requests and statuses
   ```bash
   # Generate 1,000 records
   requests = [generate_ride_request() for _ in range(1000)]
   statuses = [generate_ride_status(req["request_id"]) for req in requests]
2. Serialize data AVRO files:
   - `passenger_requests.avro`
   - `ride_statuses.avro`
   ```bash
   # Serialize to AVRO
   serialize_to_avro(requests, PASSENGER_REQUEST_SCHEMA, "passenger_requests.avro")
   serialize_to_avro(statuses, RIDE_STATUS_SCHEMA, "ride_statuses.avro")
     
### View Data
      def display_first_ten_records(filename):
         with open(filename, 'rb') as file:
            reader = fastavro.reader(file)
               for i in range(10):
               print(next(reader))

## Key Observation
1. Dynamic Fare Calculation:
   - Base fare ($5–20) multiplied by surge factors (peak hours, weather, traffic).
   - Example: A rainy Friday night ride costs $56.32 (stormy weather + high traffic).
2. Cancellation Logic:
   - 15% of requests are cancellations, with reasons like `drive_unavailbale`
3. Data Relationships:
   - `request_id` links ride statuses to their original requests.
   - Completed rides include `distance_traveled` and `driver_rating`.
   
# Milestone 2
## Spark Streaming: Azure Event Hub to Blob Storage

Stream Avro-formatted data from Azure Event Hubs (via Kafka API) using Spark Structured Streaming, process it, and persist results to Azure Blob Storage in Parquet format.

## Overview
- **Data Source**: Azure Event Hubs (Kafka API) with two topics: `passenger_requests_10` and `ride_status_10`.
- **Processing**: Deserialize Avro data using predefined schemas and perform lightweight transformations.
- **Sink**: Write processed data to Azure Blob Storage in partitioned Parquet files.
- **Fault Tolerance**: Uses Spark checkpointing to ensure exactly-once processing semantics.

## Prerequisites
- **Azure Account**: With access to Event Hubs and Blob Storage.
- **Azure Resources**:
  - Event Hub namespace with Kafka-enabled topics.
  - Blob Storage container named `group10` (or update the container name in the code).
- **Environment**:
  - Python 3.8+.
  - Java 8 (required for Spark).
  - Apache Spark 3.5.0 with Hadoop 3.
  - Libraries: `azure-eventhub`, `fastavro`, `faker`, and Spark dependencies.

## Setup
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
2. **Install Dependencies**:
   ```bash
   pip install azure-eventhub fastavro faker findspark
3. **Azure Configuration**:
   - **Event Hubs**:
      - Create an Event Hub namespace and topics (passenger_requests_10, ride_status_10).
      - Obtain the connection string with Consumer policy permissions.
   - **Blob Storage**:
      - Create a container named group10.
      - Retrieve the storage account name and access key.
4. **Update Configuration**: Replace placeholder values in the notebook with your Azure credentials:
   ```bash

## **Contributors**: Nicolas Cubillo, Pablo Gomez, Sebastian Llobet, Pablo Jaime Rivera, Jorge Rodriguez
