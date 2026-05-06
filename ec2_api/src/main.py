"""
IoT Thermal API - EC2 Instance
Receives temperature data, tracks last 10 readings, detects alert conditions
"""

import os
import json
import logging
from flask import Flask, request, jsonify
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from uuid import uuid4

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MAX_READINGS = 10
THRESHOLD_DIFFERENCE = float(os.getenv("THRESHOLD_DIFFERENCE", "5.0"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5001"))
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "iot-thermal-readings")

app = Flask(__name__)

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Device ID for partitioning (could be configurable)
DEVICE_ID = "gateway-01"


def create_table_if_not_exists():
    """Create DynamoDB table if it doesn't exist"""
    try:
        table.load()
        logger.info(f"DynamoDB table '{DYNAMODB_TABLE_NAME}' exists")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info(f"Creating DynamoDB table '{DYNAMODB_TABLE_NAME}'")
            table.create(
                KeySchema=[
                    {"AttributeName": "device_id", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"}
                ],
                AttributeDefinitions=[
                    {"AttributeName": "device_id", "AttributeType": "S"},
                    {"AttributeName": "timestamp", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST"
            )
            table.wait_until_exists()
            logger.info("DynamoDB table created successfully")
        else:
            logger.error(f"Error loading table: {e}")
            raise


def get_recent_readings(limit=MAX_READINGS):
    """Fetch last N readings from DynamoDB"""
    try:
        response = table.query(
            KeyConditionExpression="device_id = :device_id",
            ExpressionAttributeValues={":device_id": DEVICE_ID},
            ScanIndexForward=False,
            Limit=limit
        )
        return list(reversed(response.get("Items", [])))
    except ClientError as e:
        logger.error(f"Error querying readings: {e}")
        return []


def format_decimal(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: format_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [format_decimal(item) for item in obj]
    return obj


def check_alert_condition(readings):
    """
    Check if current readings have a threshold difference that exceeds limit
    Returns True if max - min > THRESHOLD_DIFFERENCE
    """
    if len(readings) < 2:
        return False, 0.0
    
    temps = [float(reading["temperature"]) for reading in readings]
    max_temp = max(temps)
    min_temp = min(temps)
    difference = max_temp - min_temp
    
    return difference > THRESHOLD_DIFFERENCE, difference


@app.route("/temperature", methods=["POST"])
def receive_temperature():
    """
    Receive temperature reading from gateway
    
    Expected JSON format:
    {
        "temperature": 25.5,
        "unit": "celsius",
        "sensor_id": "sensor_1",
        "timestamp": "2024-05-06T10:30:45.123456"  # optional
    }
    """
    try:
        data = request.get_json()
        
        if not data or "temperature" not in data:
            return jsonify({"error": "Missing temperature field"}), 400
        
        temperature = float(data.get("temperature"))
        unit = data.get("unit", "celsius")
        sensor_id = data.get("sensor_id", "default")
        sensor_timestamp = data.get("timestamp", datetime.utcnow().isoformat())
        received_at = datetime.utcnow().isoformat()
        
        # Create reading item
        reading = {
            "device_id": DEVICE_ID,
            "timestamp": sensor_timestamp,
            "temperature": Decimal(str(temperature)),
            "unit": unit,
            "sensor_id": sensor_id,
            "received_at": received_at,
            "read_id": str(uuid4())
        }
        
        # Store in DynamoDB
        try:
            table.put_item(Item=reading)
            logger.info(f"Temperature recorded: {temperature}°{unit[0].upper()}")
        except ClientError as e:
            logger.error(f"Error storing reading: {e}")
            return jsonify({"error": "Failed to store reading"}), 500
        
        # Get recent readings to check alert
        recent_readings = get_recent_readings(MAX_READINGS)
        is_alert, difference = check_alert_condition(recent_readings)
        
        logger.info(
            f"Temperature recorded: {temperature}°{unit[0].upper()} "
            f"(diff: {difference:.2f}°, alert: {is_alert})"
        )
        
        return jsonify({
            "status": "received",
            "temperature": temperature,
            "readings_count": len(recent_readings),
            "is_alert": is_alert,
            "threshold_difference": round(difference, 2) if difference else 0.0
        }), 201
        
    except ValueError as e:
        return jsonify({"error": f"Invalid temperature value: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/alert", methods=["GET"])
def get_alert_status():
    """
    Get current alert status
    
    Returns:
    {
        "is_alert": true/false,
        "current_temperature": 28.5,
        "min_temperature": 23.3,
        "max_temperature": 28.5,
        "threshold_difference": 5.2,
        "readings_count": 10,
        "threshold_limit": 5.0
    }
    """
    try:
        readings = get_recent_readings(MAX_READINGS)
        
        if len(readings) == 0:
            return jsonify({
                "is_alert": False,
                "current_temperature": None,
                "readings_count": 0,
                "message": "No readings available yet"
            }), 200
        
        temps = [float(reading["temperature"]) for reading in readings]
        current_temp = float(readings[-1]["temperature"])
        max_temp = max(temps)
        min_temp = min(temps)
        difference = max_temp - min_temp
        is_alert = difference > THRESHOLD_DIFFERENCE
        
        return jsonify({
            "is_alert": is_alert,
            "current_temperature": current_temp,
            "min_temperature": min_temp,
            "max_temperature": max_temp,
            "threshold_difference": round(difference, 2),
            "readings_count": len(readings),
            "threshold_limit": THRESHOLD_DIFFERENCE,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error getting alert status: {e}")
        return jsonify({"error": "Failed to get alert status"}), 500


@app.route("/readings", methods=["GET"])
def get_readings():
    """Get all stored temperature readings"""
    try:
        readings = get_recent_readings(MAX_READINGS)
        formatted_readings = format_decimal(readings)
        return jsonify({
            "readings": formatted_readings,
            "count": len(formatted_readings),
            "max_capacity": MAX_READINGS
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving readings: {e}")
        return jsonify({"error": "Failed to retrieve readings"}), 500


@app.route("/readings/last", methods=["GET"])
def get_last_reading():
    """Get the most recent temperature reading"""
    try:
        readings = get_recent_readings(1)
        
        if len(readings) == 0:
            return jsonify({
                "error": "No readings available yet"
            }), 404
        
        last = format_decimal(readings[0])
        return jsonify({
            "reading": last,
            "timestamp": datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving last reading: {e}")
        return jsonify({"error": "Failed to retrieve reading"}), 500


@app.route("/stats", methods=["GET"])
def get_statistics():
    """Get temperature statistics"""
    try:
        readings = get_recent_readings(MAX_READINGS)
        
        if len(readings) == 0:
            return jsonify({
                "error": "No readings available yet"
            }), 404
        
        temps = [float(reading["temperature"]) for reading in readings]
        average = sum(temps) / len(temps)
        max_temp = max(temps)
        min_temp = min(temps)
        
        return jsonify({
            "average": round(average, 2),
            "min": min_temp,
            "max": max_temp,
            "difference": round(max_temp - min_temp, 2),
            "readings_count": len(readings)
        }), 200
    except Exception as e:
        logger.error(f"Error calculating statistics: {e}")
        return jsonify({"error": "Failed to calculate statistics"}), 500


@app.route("/reset", methods=["POST"])
def reset_readings():
    """Clear all stored readings (testing/maintenance endpoint)"""
    try:
        readings = get_recent_readings(1000)  # Get up to 1000 items
        
        with table.batch_writer(
            overwrite_by_pkeys=["device_id", "timestamp"]
        ) as batch:
            for reading in readings:
                batch.delete_item(
                    Key={
                        "device_id": reading["device_id"],
                        "timestamp": reading["timestamp"]
                    }
                )
        
        logger.warning("All temperature readings cleared from DynamoDB")
        return jsonify({"status": "cleared"}), 200
    except Exception as e:
        logger.error(f"Error resetting readings: {e}")
        return jsonify({"error": "Failed to reset readings"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "IoT Thermal API",
        "timestamp": datetime.utcnow().isoformat()
    }), 200


@app.route("/config", methods=["GET"])
def get_config():
    """Return current configuration"""
    return jsonify({
        "max_readings": MAX_READINGS,
        "threshold_difference": THRESHOLD_DIFFERENCE,
        "api_host": API_HOST,
        "api_port": API_PORT
    }), 200


if __name__ == "__main__":
    logger.info(f"Starting IoT Thermal API on {API_HOST}:{API_PORT}")
    logger.info(f"Threshold difference for alerts: {THRESHOLD_DIFFERENCE}°")
    logger.info(f"Maximum readings stored: {MAX_READINGS}")
    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"DynamoDB Table: {DYNAMODB_TABLE_NAME}")
    logger.info(f"Device ID: {DEVICE_ID}")
    
    # Initialize DynamoDB table
    try:
        create_table_if_not_exists()
    except Exception as e:
        logger.error(f"Failed to initialize DynamoDB: {e}")
        logger.warning("Continuing anyway - table may already exist")
    
    app.run(host=API_HOST, port=API_PORT, debug=False)
