import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import json
import csv
import os
import uuid
import time

# --- Налаштування ---
MQTT_HOST = "81e95e73d74f4efd837464022a52355a.s1.eu.hivemq.cloud"
MQTT_PORT = 8883 
MQTT_USER = "website"
MQTT_PASS = "SKQCWqw6e8JW6Gj"
# УВАГА: Переконайтеся, що tasmota - це ваш Topic з налаштувань MQTT в Tasmota
MQTT_TOPIC = "tele/tasmota_8D7614/SENSOR" 

CSV_FILE = "data.csv"
JSON_FILE = "chart_data.json"
MAX_CHART_POINTS = 100 
TIMEOUT = 60 

data_processed = False

def update_storage(t_time, temp, hum, press):
    """Оновлює CSV та JSON файли"""
    # 1. Додаємо в CSV, тільки якщо запису ще немає
    new_record = True
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'r') as f:
            if t_time in f.read():
                print(f"DEBUG: Запис за {t_time} вже є. Пропускаю запис в CSV.")
                new_record = False

    if new_record:
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "temperature", "humidity", "pressure"])
            writer.writerow([t_time, temp, hum, press])
        print("DEBUG: Дані додано в CSV.")

    # 2. ГЕНЕРУЄМО JSON ЗАВЖДИ (щоб файл точно був)
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
                "tension": 0.3,
                "fill": False  # ПЕРЕВІРТЕ: False з великої літери!
            },
            {
                "label": "Вологість (%)",
                "data": [float(row["humidity"]) for row in recent],
                "borderColor": "#36a2eb",
                "tension": 0.3,
                "fill": False
            }
        ]
    }
    
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(chart_data, f, indent=2)
    print(f"DEBUG: Файл {JSON_FILE} згенеровано.")
    return True

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"Підключено до HiveMQ! Очікую повідомлення у {MQTT_TOPIC}...")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Помилка підключення: {rc}")

def on_message(client, userdata, msg):
    global data_processed
    try:
        raw_payload = msg.payload.decode()
        print(f"Отримано RAW JSON: {raw_payload}")
        
        payload = json.loads(raw_payload)
        t_time = payload.get("Time")
        
        # Специфічний пошук для вашого BME280
        bme = payload.get("BME280")
        
        if bme and t_time:
            temp = bme.get("Temperature")
            hum = bme.get("Humidity")
            press = bme.get("Pressure")
            
            if update_storage(t_time, temp, hum, press):
                print(f"Збережено успішно: {t_time} -> {temp}C")
                # Очищення Retain
                client.publish(MQTT_TOPIC, payload="", qos=1, retain=True)
            
            data_processed = True
            client.disconnect()
            
    except Exception as e:
        print(f"Помилка обробки: {e}")
        data_processed = True

# --- Запуск ---
client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=f"gh_{uuid.uuid4().hex[:6]}")
client.tls_set() 
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_start()

start = time.time()
while not data_processed and (time.time() - start < TIMEOUT):
    time.sleep(1)

client.loop_stop()
print("Скрипт завершив роботу.")
