from flask import Flask, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Tietokanta-asetukset
DB_CONFIG = {
    "host": "localhost",
    "user": "Irina",
    "password": "030212",
    "database": "mqtt_chat"
}

def get_connection():
    """Luo uusi tietokantayhteys."""
    return mysql.connector.connect(**DB_CONFIG)

@app.route("/api/messages", methods=["GET"])
def get_messages():
    """Hae viimeisimmät viestit tietokannasta."""
    limit = request.args.get("limit", 10, type=int)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT id, nickname, message, client_id, created_at
        FROM messages
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,)
    )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Järjestä vanhimmasta uusimpaan
    rows.reverse()

    for row in rows:
        if isinstance(row["created_at"], datetime):
            row["created_at"] = row["created_at"].isoformat()

    return jsonify(rows)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000)
