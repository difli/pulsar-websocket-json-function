import websocket
import json
import base64

# Pulsar credentials
PULSAR_SERVICE_URL = 'pulsar+ssl://pulsar-gcp-uscentral1.streaming.datastax.com:6651'
TOPIC = "wss://pulsar-gcp-uscentral1.streaming.datastax.com:8001/ws/v2/producer/persistent/streaming-demo/titan/device"
AUTH_TOKEN = "eyJh..."

# Authorization header
auth_header = {"Authorization": f"Bearer {AUTH_TOKEN}"}

# Create a WebSocket connection
ws = websocket.create_connection(TOPIC, header=auth_header)

# Prepare the message payload
message_payload = {
    "event_name": "new-register",
    "id": 4426,
    "serial_number": "tw1234xxxx",
    "ethernet_mac_address": "94:b8:66:xx:xx:xx",
    "wifi_mac_address": "c4:8b:66:xx:xx:xx",
    "device_uuid": "70971ba5-4be8-xxxx-xxxx-xxxxxxxxxxxx",
    "brand_model_id": 298,
    "market_id": 2,
    "external_id": None,
    "consumer_id": "916251xxxx",
    "firmware": "056.002.028.000",
    "for_test": False,
    "brand_model_name": "43PUSxxxx",
    "market_code": "es",
    "country": "GB",
    "brand_model_group_id": 16,
    "brand_model_group_name": ""
}

# Encode the payload in Base64
encoded_payload = base64.b64encode(json.dumps(message_payload).encode("utf-8")).decode("utf-8")

# Prepare the message to send
message = {
    "payload": encoded_payload,
    "properties": {
        "source": "websocket",
        "type": "device"
    },
    "context": 5
}

# Send the message via WebSocket
ws.send(json.dumps(message))

# Receive the server response
response = json.loads(ws.recv())
if response["result"] == "ok":
    print("Message published successfully.")
else:
    print(f"Failed to publish message: {response}")

# Close the WebSocket connection
ws.close()
