import paho.mqtt.client as mqtt
import json
import csv
import os
import uuid

# Налаштування
MQTT_HOST = "81e95e73d74f4efd837464022a52355a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883 
MQTT_USER = "website"
MQTT_PASS = "SKQCWqw6e8JW6Gj"
MQTT_TOPIC = "tele/tasmota/SENSOR"

CSV_FILE = "data.csv"
JSON_FILE = "chart_data.json"
MAX_CHART_POINTS = 100

def update_files(tasmota_time, temp, hum):
    # 1. Запис в CSV
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "temperature", "humidity"])
        writer.writerow([tasmota_time, temp, hum])
    
    # 2. Читання історії для JSON
    history = []
    with open(CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        history = list(reader)
    
    # 3. Формування JSON для Chart.js
    recent = history[-MAX_CHART_POINTS:]
    chart_data = {
        "labels": [row["timestamp"] for row in recent],
        "datasets": [
            {"label": "Temp", "data": [float(row["temperature"]) for row in recent], "borderColor": "#ff6384"},
            {"label": "Hum", "data": [float(row["humidity"]) for row in recent], "borderColor": "#36a2eb"}
        ]
    }
    with open(JSON_FILE, 'w') as f:
        json.dump(chart_data, f, indent=2)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        t_time = payload.get("Time")
        sensor = payload.get("AM2301") or payload.get("DS18B20") or payload.get("BME280")
        
        if sensor and t_time:
            update_files(t_time, sensor.get("Temperature"), sensor.get("Humidity", 0))
            client.publish(MQTT_TOPIC, payload="", qos=1, retain=True) # Очищення
            client.disconnect()
    except Exception as e:
        print(f"Error: {e}")
        client.disconnect()

client = mqtt.Client(client_id=f"gh_bot_{uuid.uuid4().hex[:6]}", transport="tcp")
client.tls_set()
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = lambda c,u,f,rc: c.subscribe(MQTT_TOPIC)
client.on_message = on_message

client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()
