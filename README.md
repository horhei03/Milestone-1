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
2. Serialize data AVRO files:
   - `passenger_requests.avro`
   - `ride_statuses.avro`
```bash
# Generate 1,000 records
requests = [generate_ride_request() for _ in range(1000)]
statuses = [generate_ride_status(req["request_id"]) for req in requests]

# Serialize to AVRO
serialize_to_avro(requests, PASSENGER_REQUEST_SCHEMA, "passenger_requests.avro")
serialize_to_avro(statuses, RIDE_STATUS_SCHEMA, "ride_statuses.avro")
