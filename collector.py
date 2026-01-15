import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import json
import csv
import os
import uuid
import time # Додаємо бібліотеку для відстеження часу

# --- Налаштування ---
MQTT_HOST = "81e95e73d74f4efd837464022a52355a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883 
MQTT_USER = "website"
MQTT_PASS = "SKQCWqw6e8JW6Gj"
MQTT_TOPIC = "tele/tasmota/SENSOR"

CSV_FILE = "data.csv"
JSON_FILE = "chart_data.json"
MAX_CHART_POINTS = 100 
TIMEOUT = 60 # Скрипт завершиться через 60 секунд, якщо нічого не отримає

# Прапорець для виходу з циклу
data_received = False

def update_storage(tasmota_time, temp, hum):
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "temperature", "humidity"])
        writer.writerow([tasmota_time, temp, hum])
    
    history = []
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r') as f:
            reader = csv.DictReader(f)
            history = list(reader)
    
    recent = history[-MAX_CHART_POINTS:]
    chart_data = {
        "labels": [row["timestamp"] for row in recent],
        "datasets": [
            {
                "label": "Температура (°C)",
                "data": [float(row["temperature"]) for row in recent],
                "borderColor": "#ff6384",
                "backgroundColor": "rgba(255, 99, 132, 0.2)",
                "tension": 0.3,
                "fill": True
            },
            {
                "label": "Вологість (%)",
                "data": [float(row["humidity"]) for row in recent],
                "borderColor": "#36a2eb",
                "backgroundColor": "rgba(54, 162, 235, 0.2)",
                "tension": 0.3,
                "fill": True
            }
        ]
    }
    with open(JSON_FILE, 'w') as f:
        json.dump(chart_data, f, indent=2)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"Підключено! Очікування даних у топіку: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Помилка підключення: {rc}")

def on_message(client, userdata, msg):
    global data_received
    try:
        payload_str = msg.payload.decode()
        if not payload_str: return
            
        payload = json.loads(payload_str)
        t_time = payload.get("Time")
        sensor = payload.get("AM2301") or payload.get("DS18B20") or payload.get("BME280") or payload.get("SHT3X")
        
        if sensor and t_time:
            temp = sensor.get("Temperature")
            hum = sensor.get("Humidity", 0)
            print(f"Дані отримано: {temp}°C")
            update_storage(t_time, temp, hum)
            
            # Очищуємо Retain і ставимо прапорець завершення
            client.publish(MQTT_TOPIC, payload="", qos=1, retain=True)
            data_received = True 
            
    except Exception as e:
        print(f"Помилка: {e}")
        data_received = True

# --- Запуск ---
client_id = f"gh_action_{uuid.uuid4().hex[:6]}"
client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=client_id)
client.tls_set() 
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST, MQTT_PORT, 60)

# Замість loop_forever() використовуємо свій цикл з таймаутом
start_time = time.time()
client.loop_start() # Запускає фоновий потік MQTT

print(f"Скрипт запущено. Таймаут: {TIMEOUT}с")

while not data_received:
    if time.time() - start_time > TIMEOUT:
        print("Час очікування вичерпано. Дані не знайдено.")
        break
    time.sleep(1)

client.loop_stop()
client.disconnect()
print("Роботу завершено.")