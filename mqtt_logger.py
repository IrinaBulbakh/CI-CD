"""
MQTT to MySQL Logger
"""

import json
import logging
import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import pooling

# MQTT-asetukset
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "chat/messages"

# Tietokanta-asetukset 
DB_CONFIG = {
    "host": "localhost",
    "user": "Irina",
    "password": "030212",  
    "database": "mqtt_chat"
}

# Lokitus
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Tietokantapooli
db_pool = pooling.MySQLConnectionPool(
    pool_name="mqtt_pool",
    pool_size=5,
    **DB_CONFIG
)


def save_message(nickname: str, message: str, client_id: str) -> None:
    """Tallenna viesti tietokantaan."""
    conn = None
    cursor = None
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO messages (nickname, message, client_id)
            VALUES (%s, %s, %s)
        """
        cursor.execute(query, (nickname, message, client_id))
        conn.commit()
        logger.info("Tallennettu: [%s] %.50s", nickname, message)
    except mysql.connector.Error as err:
        logger.error("Tietokantavirhe: %s", err)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def on_connect(client, userdata, flags, rc):
    """Kutsutaan kun MQTT-yhteys muodostuu."""
    if rc == 0:
        logger.info("Yhdistetty MQTT-brokeriin")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error("Yhteysvirhe, rc=%s", rc)


def on_message(client, userdata, msg):
    """Käsittele saapuva MQTT-viesti."""
    try:
        data = json.loads(msg.payload.decode("utf-8"))

        print("RECEIVED RAW:", msg.payload.decode("utf-8"))
        print("RECEIVED PARSED:", data)

        nickname = (data.get("nickname") or "Tuntematon")[:50]

        text = data.get("text") or ""
        client_id = (data.get("clientId") or "")[:100]

        if text:
            save_message(nickname, text, client_id)

    except Exception as e:
        logger.error("Virhe viestissä: %s", e)

def main():
    """Pääohjelma."""
    logger.info("MQTT Logger käynnistyy...")

    # Luo MQTT-asiakas
    client = mqtt.Client(client_id="mqtt_logger")
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
