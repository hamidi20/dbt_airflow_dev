from kafka import KafkaProducer
import psycopg2
import json
from datetime import datetime

# ==========================================
# SOURCE DB CONNECTION
# ==========================================

conn = psycopg2.connect(
    host="192.168.10.120",
    dbname="api-service",
    user="postgres",
    password="Admin@postgres123"
)

cur = conn.cursor()

# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# ==========================================
# REPLAY CUSTOMERS
# ==========================================

print("REPLAY CUSTOMERS...")

cur.execute("""
    SELECT
        customer_id,
        full_name,
        email,
        phone,
        created_at,
        updated_at,
        is_deleted
    FROM customers
""")

rows = cur.fetchall()

for row in rows:

    event = {
        "event_type": "customer_created",
        "event_time": datetime.now().isoformat(),
        "payload": {
            "customer_id": row[0],
            "full_name": row[1],
            "email": row[2],
            "phone": row[3],
            "created_at": str(row[4]),
            "updated_at": str(row[5]),
            "is_deleted": row[6]
        }
    }

    producer.send("customers_topic", event)

print(f"SUCCESS REPLAY CUSTOMERS: {len(rows)}")

# ==========================================
# REPLAY ORDERS
# ==========================================

print("REPLAY ORDERS...")

cur.execute("""
    SELECT
        order_id,
        customer_id,
        product_name,
        qty,
        price,
        order_date
    FROM orders
""")

rows = cur.fetchall()

for row in rows:

    event = {
        "event_type": "order_created",
        "event_time": datetime.now().isoformat(),
        "payload": {
            "order_id": row[0],
            "customer_id": row[1],
            "product_name": row[2],
            "qty": row[3],
            "price": float(row[4]),
            "order_date": str(row[5])
        }
    }

    producer.send("orders_topic", event)

print(f"SUCCESS REPLAY ORDERS: {len(rows)}")

# ==========================================
# REPLAY PAYMENTS
# ==========================================

print("REPLAY PAYMENTS...")

cur.execute("""
    SELECT
        payment_id,
        order_id,
        payment_method,
        amount,
        payment_date
    FROM payments
""")

rows = cur.fetchall()

for row in rows:

    event = {
        "event_type": "payment_created",
        "event_time": datetime.now().isoformat(),
        "payload": {
            "payment_id": row[0],
            "order_id": row[1],
            "payment_method": row[2],
            "amount": float(row[3]),
            "payment_date": str(row[4])
        }
    }

    producer.send("payments_topic", event)

print(f"SUCCESS REPLAY PAYMENTS: {len(rows)}")

producer.flush()

print("DONE REPLAY ALL DATA")