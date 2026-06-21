from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from typing import Optional
from kafka import KafkaProducer
from datetime import datetime
import json

# ==================================================
# FASTAPI APP
# ==================================================

app = FastAPI(
    title="API Service",
    version="1.0.0"
)

# ==================================================
# DATABASE CONNECTION
# ==================================================

DATABASE_URL = "postgresql://postgres:Admin%40postgres123@192.168.10.120/api-service"

engine = create_engine(DATABASE_URL)

# ==================================================
# KAFKA CONNECTION
# ==================================================

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# ==================================================
# HELPER FUNCTION
# ==================================================

def publish_event(topic, payload):

    try:

        producer.send(topic, payload)

        producer.flush()

    except Exception as e:

        print("KAFKA ERROR:", e)

# ==================================================
# EVENT BUILDER
# ==================================================

def build_event(event_type, payload):

    return {
        "event_type": event_type,
        "event_time": datetime.now().isoformat(),
        "source": "api-service",
        "version": 1,
        "payload": payload
    }

# ==================================================
# PYDANTIC MODELS
# ==================================================

class CustomerCreate(BaseModel):
    full_name: str
    email: str
    phone: Optional[str] = None


class CustomerUpdate(BaseModel):
    full_name: str
    email: str
    phone: Optional[str] = None


class OrderCreate(BaseModel):
    customer_id: int
    product_name: str
    qty: int
    price: float


class OrderUpdate(BaseModel):
    customer_id: int
    product_name: str
    qty: int
    price: float


class PaymentCreate(BaseModel):
    order_id: int
    payment_method: str
    amount: float


class PaymentUpdate(BaseModel):
    order_id: int
    payment_method: str
    amount: float

# ==================================================
# HOME
# ==================================================

@app.get("/")
def home():

    return {
        "message": "API Running"
    }

# ==================================================
# HEALTH CHECK
# ==================================================

@app.get("/health")
def health():

    return {
        "status": "healthy"
    }

# ==================================================
# CUSTOMERS CRUD + KAFKA
# ==================================================

@app.post("/api/customers")
def create_customer(data: CustomerCreate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                INSERT INTO customers
                (
                    full_name,
                    email,
                    phone
                )
                VALUES
                (
                    :full_name,
                    :email,
                    :phone
                )
                RETURNING customer_id
            """),
            data.dict()
        )

        customer_id = result.scalar()

    event = build_event(
        "customer_created",
        {
            "customer_id": customer_id,
            "full_name": data.full_name,
            "email": data.email,
            "phone": data.phone,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "is_deleted": False
        }
    )

    publish_event("customers_topic", event)

    return {
        "message": "customer created",
        "customer_id": customer_id
    }


@app.put("/api/customers/{customer_id}")
def update_customer(customer_id: int, data: CustomerUpdate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                UPDATE customers
                SET
                    full_name=:full_name,
                    email=:email,
                    phone=:phone,
                    updated_at=CURRENT_TIMESTAMP
                WHERE customer_id=:id
                AND is_deleted=false
            """),
            {
                "id": customer_id,
                **data.dict()
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="customer not found"
        )

    event = build_event(
        "customer_updated",
        {
            "customer_id": customer_id,
            "full_name": data.full_name,
            "email": data.email,
            "phone": data.phone,
            "updated_at": datetime.now().isoformat(),
            "is_deleted": False
        }
    )

    publish_event("customers_topic", event)

    return {
        "message": "customer updated"
    }


@app.delete("/api/customers/{customer_id}")
def delete_customer(customer_id: int):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                UPDATE customers
                SET
                    is_deleted=true,
                    updated_at=CURRENT_TIMESTAMP
                WHERE customer_id=:id
                AND is_deleted=false
            """),
            {
                "id": customer_id
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="customer not found"
        )

    event = build_event(
        "customer_deleted",
        {
            "customer_id": customer_id,
            "updated_at": datetime.now().isoformat(),
            "is_deleted": True
        }
    )

    publish_event("customers_topic", event)

    return {
        "message": "customer deleted"
    }

# ==================================================
# ORDERS CRUD + KAFKA
# ==================================================

@app.post("/api/orders")
def create_order(data: OrderCreate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                INSERT INTO orders
                (
                    customer_id,
                    product_name,
                    qty,
                    price
                )
                VALUES
                (
                    :customer_id,
                    :product_name,
                    :qty,
                    :price
                )
                RETURNING order_id
            """),
            data.dict()
        )

        order_id = result.scalar()

    event = build_event(
        "order_created",
        {
            "order_id": order_id,
            "customer_id": data.customer_id,
            "product_name": data.product_name,
            "qty": data.qty,
            "price": data.price,
            "order_date": datetime.now().isoformat()
        }
    )

    publish_event("orders_topic", event)

    return {
        "message": "order created",
        "order_id": order_id
    }


@app.put("/api/orders/{order_id}")
def update_order(order_id: int, data: OrderUpdate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                UPDATE orders
                SET
                    customer_id=:customer_id,
                    product_name=:product_name,
                    qty=:qty,
                    price=:price
                WHERE order_id=:id
            """),
            {
                "id": order_id,
                **data.dict()
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="order not found"
        )

    event = build_event(
        "order_updated",
        {
            "order_id": order_id,
            "customer_id": data.customer_id,
            "product_name": data.product_name,
            "qty": data.qty,
            "price": data.price,
            "order_date": datetime.now().isoformat()
        }
    )

    publish_event("orders_topic", event)

    return {
        "message": "order updated"
    }


@app.delete("/api/orders/{order_id}")
def delete_order(order_id: int):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                DELETE FROM orders
                WHERE order_id=:id
            """),
            {
                "id": order_id
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="order not found"
        )

    event = build_event(
        "order_deleted",
        {
            "order_id": order_id
        }
    )

    publish_event("orders_topic", event)

    return {
        "message": "order deleted"
    }

# ==================================================
# PAYMENTS CRUD + KAFKA
# ==================================================

@app.post("/api/payments")
def create_payment(data: PaymentCreate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                INSERT INTO payments
                (
                    order_id,
                    payment_method,
                    amount
                )
                VALUES
                (
                    :order_id,
                    :payment_method,
                    :amount
                )
                RETURNING payment_id
            """),
            data.dict()
        )

        payment_id = result.scalar()

    event = build_event(
        "payment_created",
        {
            "payment_id": payment_id,
            "order_id": data.order_id,
            "payment_method": data.payment_method,
            "amount": data.amount,
            "payment_date": datetime.now().isoformat()
        }
    )

    publish_event("payments_topic", event)

    return {
        "message": "payment created",
        "payment_id": payment_id
    }


@app.put("/api/payments/{payment_id}")
def update_payment(payment_id: int, data: PaymentUpdate):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                UPDATE payments
                SET
                    order_id=:order_id,
                    payment_method=:payment_method,
                    amount=:amount
                WHERE payment_id=:id
            """),
            {
                "id": payment_id,
                **data.dict()
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="payment not found"
        )

    event = build_event(
        "payment_updated",
        {
            "payment_id": payment_id,
            "order_id": data.order_id,
            "payment_method": data.payment_method,
            "amount": data.amount,
            "payment_date": datetime.now().isoformat()
        }
    )

    publish_event("payments_topic", event)

    return {
        "message": "payment updated"
    }


@app.delete("/api/payments/{payment_id}")
def delete_payment(payment_id: int):

    with engine.begin() as conn:

        result = conn.execute(
            text("""
                DELETE FROM payments
                WHERE payment_id=:id
            """),
            {
                "id": payment_id
            }
        )

    if result.rowcount == 0:

        raise HTTPException(
            status_code=404,
            detail="payment not found"
        )

    event = build_event(
        "payment_deleted",
        {
            "payment_id": payment_id
        }
    )

    publish_event("payments_topic", event)

    return {
        "message": "payment deleted"
    }