from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class OrderItemCreate(BaseModel):
    product_id: str
    quantity: int

class OrderCreate(BaseModel):
    customer_id: str
    payment_method: Optional[str] = None
    items: List[OrderItemCreate]

class OrderOut(BaseModel):
    order_id: str
    total_amount: float
    status: str
# À ajouter dans schemas.py
class CustomerCreate(BaseModel):
    first_name: str
    last_name: str
    email: str
    country: Optional[str] = None
    city: Optional[str] = None

class CustomerOut(CustomerCreate):
    customer_id: str
    created_at: datetime

class ProductCreate(BaseModel):
    name: str
    category: str
    brand: str
    price: float

class ProductOut(ProductCreate):
    product_id: str
# ---- Domain: Cart ----
class CartItemCreate(BaseModel):
    product_id: str
    quantity: int

class CartItemOut(BaseModel):
    id: int
    cart_id: str
    product_id: str
    quantity: int

class CartCreate(BaseModel):
    customer_id: str

class CartOut(BaseModel):
    cart_id: str
    customer_id: str
    status: str
    created_at: datetime

# ---- Domain: Payment ----
class PaymentCreate(BaseModel):
    order_id: str
    payment_method: str
    amount: float

class PaymentOut(BaseModel):
    payment_id: str
    order_id: str
    status: str
    amount: float
    payment_date: datetime

# ---- Domain: Shipment ----
class ShipmentCreate(BaseModel):
    order_id: str

class ShipmentOut(BaseModel):
    shipment_id: str
    order_id: str
    carrier: str
    tracking_number: str
    status: str
    delivery_date: Optional[datetime] = None
class WarehouseCreate(BaseModel):
    name: str
    city: str

class WarehouseOut(WarehouseCreate):
    id: int
class InventoryUpdate(BaseModel):
    product_id: str
    warehouse_id: int
    quantity: int