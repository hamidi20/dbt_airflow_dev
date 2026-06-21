from kafka import KafkaConsumer
import psycopg2
import json
import time
from datetime import datetime

# ==================================================
# CONFIG
# ==================================================

KAFKA_BROKER = "kafka:9092"

POSTGRES_CONFIG = {
    "host": "192.168.10.120",
    "dbname": "master",
    "user": "postgres",
    "password": "Admin@postgres123"
}

# ==================================================
# CONNECT KAFKA
# ==================================================

while True:

    try:

        consumer = KafkaConsumer(
            "customers_topic",
            "orders_topic",
            "payments_topic",

            bootstrap_servers=KAFKA_BROKER,

            auto_offset_reset="earliest",

            enable_auto_commit=True,

            group_id="ods_sync_group",

            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

        print("SUCCESS CONNECT KAFKA")

        break

    except Exception as e:

        print("KAFKA CONNECTION ERROR:", e)

        time.sleep(5)

# ==================================================
# CONNECT POSTGRES MASTER DB
# ==================================================

while True:

    try:

        conn = psycopg2.connect(**POSTGRES_CONFIG)

        cur = conn.cursor()

        print("SUCCESS CONNECT MASTER DB")

        break

    except Exception as e:

        print("DB CONNECTION ERROR:", e)

        time.sleep(5)

# ==================================================
# START CONSUMER
# ==================================================

print("START CONSUMER...")

# ==================================================
# PROCESS MESSAGE
# ==================================================

for msg in consumer:

    try:

        topic = msg.topic

        data = msg.value

        event_type = data["event_type"]

        payload = data["payload"]

        print(
            f"{datetime.now()} | "
            f"TOPIC={topic} | "
            f"EVENT={event_type}"
        )

        # ==================================================
        # CUSTOMERS
        # ==================================================

        if topic == "customers_topic":

            # ==============================================
            # DELETE
            # ==============================================

            if event_type == "customer_deleted":

                cur.execute("""
                    UPDATE ods_customers
                    SET
                        is_deleted = true,
                        updated_at = %s
                    WHERE customer_id = %s
                """, (
                    payload["updated_at"],
                    payload["customer_id"]
                ))

            # ==============================================
            # UPSERT
            # ==============================================

            else:

                cur.execute("""
                    INSERT INTO ods_customers
                    (
                        customer_id,
                        full_name,
                        email,
                        phone,
                        created_at,
                        updated_at,
                        is_deleted
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s,%s,%s
                    )

                    ON CONFLICT (customer_id)

                    DO UPDATE SET
                        full_name = EXCLUDED.full_name,
                        email = EXCLUDED.email,
                        phone = EXCLUDED.phone,
                        updated_at = EXCLUDED.updated_at,
                        is_deleted = EXCLUDED.is_deleted
                """, (
                    payload["customer_id"],
                    payload.get("full_name"),
                    payload.get("email"),
                    payload.get("phone"),
                    payload.get("created_at"),
                    payload.get("updated_at"),
                    payload.get("is_deleted", False)
                ))

        # ==================================================
        # ORDERS
        # ==================================================

        elif topic == "orders_topic":

            # ==============================================
            # DELETE
            # ==============================================

            if event_type == "order_deleted":

                cur.execute("""
                    DELETE FROM ods_orders
                    WHERE order_id = %s
                """, (
                    payload["order_id"],
                ))

            # ==============================================
            # UPSERT
            # ==============================================

            else:

                cur.execute("""
                    INSERT INTO ods_orders
                    (
                        order_id,
                        customer_id,
                        product_name,
                        qty,
                        price,
                        order_date
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s,%s
                    )

                    ON CONFLICT (order_id)

                    DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        product_name = EXCLUDED.product_name,
                        qty = EXCLUDED.qty,
                        price = EXCLUDED.price,
                        order_date = EXCLUDED.order_date
                """, (
                    payload["order_id"],
                    payload["customer_id"],
                    payload["product_name"],
                    payload["qty"],
                    payload["price"],
                    payload.get("order_date")
                ))

        # ==================================================
        # PAYMENTS
        # ==================================================

        elif topic == "payments_topic":

            # ==============================================
            # DELETE
            # ==============================================

            if event_type == "payment_deleted":

                cur.execute("""
                    DELETE FROM ods_payments
                    WHERE payment_id = %s
                """, (
                    payload["payment_id"],
                ))

            # ==============================================
            # UPSERT
            # ==============================================

            else:

                cur.execute("""
                    INSERT INTO ods_payments
                    (
                        payment_id,
                        order_id,
                        payment_method,
                        amount,
                        payment_date
                    )
                    VALUES
                    (
                        %s,%s,%s,%s,%s
                    )

                    ON CONFLICT (payment_id)

                    DO UPDATE SET
                        order_id = EXCLUDED.order_id,
                        payment_method = EXCLUDED.payment_method,
                        amount = EXCLUDED.amount,
                        payment_date = EXCLUDED.payment_date
                """, (
                    payload["payment_id"],
                    payload["order_id"],
                    payload["payment_method"],
                    payload["amount"],
                    payload.get("payment_date")
                ))

        # ==================================================
        # COMMIT
        # ==================================================

        conn.commit()

        print("SUCCESS COMMIT")

    except Exception as e:

        conn.rollback()

        print("ERROR:", e)