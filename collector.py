import paho.mqtt.client as mqtt
import json
import csv
import os
import uuid
from datetime import datetime

MQTT_HOST = "81e95e73d74f4efd837464022a52355a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883 # Стандартний порт для SSL/TLS (MQTTS)
MQTT_USER = "website"
MQTT_PASS = "SKQCWqw6e8JW6Gj"
MQTT_TOPIC = "tele/tasmota/SENSOR"

CSV_FILE = "data.csv"
JSON_FILE = "chart_data.json"
MAX_CHART_POINTS = 100

def update_files(tasmota_time, temp, hum):
    # Тепер ми отримуємо tasmota_time як аргумент
    timestamp = tasmota_time 
    
    # 1. Записуємо в CSV
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "temperature", "humidity"])
        writer.writerow([timestamp, temp, hum])
    
    # 2. Оновлюємо JSON для Chart.js
    # (код оновлення JSON залишається без змін, він візьме tasmota_time з CSV)
    history = []
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r') as f:
            reader = csv.DictReader(f)
            history = list(reader)
    
    recent_data = history[-MAX_CHART_POINTS:]
    chart_json = {
        "labels": [row["timestamp"] for row in recent_data],
        "datasets": [
            {
                "label": "Температура (°C)",
                "data": [float(row["temperature"]) for row in recent_data],
                "borderColor": "rgb(255, 99, 132)",
                "tension": 0.1
            },
            {
                "label": "Вологість (%)",
                "data": [float(row["humidity"]) for row in recent_data],
                "borderColor": "rgb(54, 162, 235)",
                "tension": 0.1
            }
        ]
    }
    with open(JSON_FILE, 'w') as f:
        json.dump(chart_json, f, indent=2)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Підключено до HiveMQ успішно!")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Помилка підключення, код: {rc}")

def on_message(client, userdata, msg):
    try:
        data_str = msg.payload.decode()
        if not data_str:
            return
            
        payload = json.loads(data_str)
        
        # Отримуємо час безпосередньо з JSON Tasmota
        # Зазвичай це поле "Time": "2023-10-27T10:45:00"
        tasmota_time = payload.get("Time")
        
        # Пошук даних сенсора
        sensor_data = payload.get("AM2301") or payload.get("DS18B20") or payload.get("BME280")
        
        if sensor_data and tasmota_time:
            temp = sensor_data.get("Temperature")
            hum = sensor_data.get("Humidity", 0)
            
            # Передаємо час із Tasmota у функцію запису
            update_files(tasmota_time, temp, hum)
            print(f"Дані від {tasmota_time}: {temp}°C, {hum}%")
            
            # Очищуємо Retain
            client.publish(MQTT_TOPIC, payload="", qos=1, retain=True)
            client.disconnect()
            
    except Exception as e:
        print(f"Помилка обробки: {e}")
        client.disconnect()

# Налаштування клієнта
client_id = f"python_bot_{uuid.uuid4().hex[:6]}"
client = mqtt.Client(client_id=client_id, transport="tcp") # Для 8883 використовуємо tcp
client.tls_set() # Активуємо TLS для HiveMQ Cloud
client.username_pw_set(MQTT_USER, MQTT_PASS)

client.on_connect = on_connect
client.on_message = on_message

print("Очікування повідомлення...")
client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_forever()
