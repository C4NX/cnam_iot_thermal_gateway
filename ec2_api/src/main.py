"""
IoT Thermal API - EC2 Instance
Receives temperature data, tracks last 10 readings, detects alert conditions
"""

import os
import json
import logging
from flask import Flask, request, jsonify
from collections import deque
from datetime import datetime
from threading import Lock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MAX_READINGS = 10
THRESHOLD_DIFFERENCE = 5.0  # degrees difference to trigger alert
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5001"))

app = Flask(__name__)

# Thread-safe storage for temperature readings
readings_lock = Lock()
temperature_readings = deque(maxlen=MAX_READINGS)


def check_alert_condition():
    """
    Check if current readings have a threshold difference that exceeds limit
    Returns True if max - min > THRESHOLD_DIFFERENCE
    """
    if len(temperature_readings) < 2:
        return False
    
    temps = [reading["temperature"] for reading in temperature_readings]
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
        timestamp = data.get("timestamp", datetime.utcnow().isoformat())
        
        # Store reading
        with readings_lock:
            reading = {
                "temperature": temperature,
                "unit": unit,
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "received_at": datetime.utcnow().isoformat()
            }
            temperature_readings.append(reading)
        
        # Check alert condition
        is_alert, difference = check_alert_condition()
        
        logger.info(
            f"Temperature recorded: {temperature}°{unit[0].upper()} "
            f"(diff: {difference:.2f}°, alert: {is_alert})"
        )
        
        return jsonify({
            "status": "received",
            "temperature": temperature,
            "readings_count": len(temperature_readings),
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
    with readings_lock:
        if len(temperature_readings) == 0:
            return jsonify({
                "is_alert": False,
                "current_temperature": None,
                "readings_count": 0,
                "message": "No readings available yet"
            }), 200
        
        temps = [reading["temperature"] for reading in temperature_readings]
        current_temp = temperature_readings[-1]["temperature"]
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
            "readings_count": len(temperature_readings),
            "threshold_limit": THRESHOLD_DIFFERENCE,
            "timestamp": datetime.utcnow().isoformat()
        }), 200


@app.route("/readings", methods=["GET"])
def get_readings():
    """Get all stored temperature readings"""
    with readings_lock:
        return jsonify({
            "readings": list(temperature_readings),
            "count": len(temperature_readings),
            "max_capacity": MAX_READINGS
        }), 200


@app.route("/readings/last", methods=["GET"])
def get_last_reading():
    """Get the most recent temperature reading"""
    with readings_lock:
        if len(temperature_readings) == 0:
            return jsonify({
                "error": "No readings available yet"
            }), 404
        
        last = temperature_readings[-1]
        return jsonify({
            "reading": last,
            "timestamp": datetime.utcnow().isoformat()
        }), 200


@app.route("/stats", methods=["GET"])
def get_statistics():
    """Get temperature statistics"""
    with readings_lock:
        if len(temperature_readings) == 0:
            return jsonify({
                "error": "No readings available yet"
            }), 404
        
        temps = [reading["temperature"] for reading in temperature_readings]
        average = sum(temps) / len(temps)
        max_temp = max(temps)
        min_temp = min(temps)
        
        return jsonify({
            "average": round(average, 2),
            "min": min_temp,
            "max": max_temp,
            "difference": round(max_temp - min_temp, 2),
            "readings_count": len(temperature_readings)
        }), 200


@app.route("/reset", methods=["POST"])
def reset_readings():
    """Clear all stored readings (testing/maintenance endpoint)"""
    with readings_lock:
        temperature_readings.clear()
    
    logger.warning("Temperature readings cleared")
    return jsonify({"status": "cleared"}), 200


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
    app.run(host=API_HOST, port=API_PORT, debug=False)
