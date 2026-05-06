"""
IoT Thermal Gateway - M5 Stack Nano
Receives temperature data from sensors and forwards to EC2 API
"""

import os
import json
import logging
from flask import Flask, request, jsonify
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
EC2_API_URL = os.getenv("EC2_API_URL", "http://localhost:5001")
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "5000"))
TIMEOUT = 5  # seconds

app = Flask(__name__)


@app.route("/sensor", methods=["POST"])
def receive_sensor_data():
    """
    Receive temperature data from M5 Stack Nano sensor
    
    Expected JSON format:
    {
        "temperature": 25.5,
        "unit": "celsius",
        "sensor_id": "sensor_1"  # optional
    }
    """
    try:
        data = request.get_json()
        
        if not data or "temperature" not in data:
            return jsonify({"error": "Missing temperature field"}), 400
        
        temperature = float(data.get("temperature"))
        unit = data.get("unit", "celsius")
        sensor_id = data.get("sensor_id", "default")
        
        logger.info(
            f"Received temperature: {temperature}°{unit[0].upper()} "
            f"from sensor: {sensor_id}"
        )
        
        # Forward to EC2 API
        ec2_payload = {
            "temperature": temperature,
            "unit": unit,
            "sensor_id": sensor_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        try:
            response = requests.post(
                f"{EC2_API_URL}/temperature",
                json=ec2_payload,
                timeout=TIMEOUT
            )
            response.raise_for_status()
            logger.info(f"Successfully forwarded to EC2 API")
            
            return jsonify({
                "status": "success",
                "temperature": temperature,
                "forwarded_to_ec2": True
            }), 200
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to forward to EC2 API: {str(e)}")
            return jsonify({
                "status": "received_locally",
                "temperature": temperature,
                "forwarded_to_ec2": False,
                "error": f"EC2 connection failed: {str(e)}"
            }), 207  # 207 Multi-Status
        
    except ValueError as e:
        return jsonify({"error": f"Invalid temperature value: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/alert", methods=["GET"])
def get_alert():
    """
    Get alert status from EC2 API
    Used by M5 Stack monitor to check if temperature is in alert state
    """
    try:
        response = requests.get(
            f"{EC2_API_URL}/alert",
            timeout=TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Alert status retrieved: is_alert={data.get('is_alert')}")
        
        return jsonify(data), 200
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get alert status from EC2 API: {str(e)}")
        return jsonify({
            "error": "Failed to get alert status",
            "is_alert": False,
            "message": f"EC2 connection failed: {str(e)}"
        }), 503


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    ec2_status = "unknown"
    
    try:
        response = requests.get(
            f"{EC2_API_URL}/health",
            timeout=2
        )
        ec2_status = "connected" if response.status_code == 200 else "unavailable"
    except requests.exceptions.RequestException:
        ec2_status = "unavailable"
    
    return jsonify({
        "status": "healthy",
        "gateway": "M5 Stack Nano Gateway",
        "ec2_api": ec2_status,
        "timestamp": datetime.utcnow().isoformat()
    }), 200


@app.route("/config", methods=["GET"])
def get_config():
    """Return current configuration"""
    return jsonify({
        "ec2_api_url": EC2_API_URL,
        "gateway_host": GATEWAY_HOST,
        "gateway_port": GATEWAY_PORT,
        "timeout": TIMEOUT
    }), 200


if __name__ == "__main__":
    logger.info(
        f"Starting IoT Thermal Gateway on {GATEWAY_HOST}:{GATEWAY_PORT}"
    )
    logger.info(f"EC2 API endpoint: {EC2_API_URL}")
    app.run(host=GATEWAY_HOST, port=GATEWAY_PORT, debug=False)
