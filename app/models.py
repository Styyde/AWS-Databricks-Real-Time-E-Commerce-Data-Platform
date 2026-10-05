from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
import uuid

Base = declarative_base()

class Customer(Base):
    __tablename__ = "customers"
    customer_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    first_name = Column(String)
    last_name = Column(String)
    email = Column(String, unique=True)
    country = Column(String, nullable=True)
    city = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    orders = relationship("Order", back_populates="customer")

class Product(Base):
    __tablename__ = "products"
    product_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String)
    category = Column(String)
    brand = Column(String)
    price = Column(Float)
    inventory_items = relationship("Inventory", back_populates="product")

    order_items = relationship("OrderItem", back_populates="product")

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, ForeignKey("customers.customer_id"))
    order_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="completed")
    total_amount = Column(Float)
    payment_method = Column(String, nullable=True)

    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order")

class OrderItem(Base):
    __tablename__ = "order_items"
    order_id = Column(String, ForeignKey("orders.order_id"), primary_key=True)
    product_id = Column(String, ForeignKey("products.product_id"), primary_key=True)
    quantity = Column(Integer)
    unit_price = Column(Float)

    order = relationship("Order", back_populates="order_items")
    product = relationship("Product", back_populates="order_items")
# Nouveau modèle
class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    city = Column(String)

    inventory_items = relationship("Inventory", back_populates="warehouse")

# Nouveau modèle - Scinder stock_quantity de Product vers Inventory
class Inventory(Base):
    __tablename__ = "inventory"
    id = Column(Integer, primary_key=True)
    product_id = Column(String, ForeignKey("products.product_id"))
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    quantity = Column(Integer)
    
    product = relationship("Product", back_populates="inventory_items")
    warehouse = relationship("Warehouse", back_populates="inventory_items")

class Cart(Base):
    __tablename__ = "carts"
    cart_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, ForeignKey("customers.customer_id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default="active") # active, abandoned, converted

    items = relationship("CartItem", back_populates="cart")

class CartItem(Base):
    __tablename__ = "cart_items"
    id = Column(Integer, primary_key=True)
    cart_id = Column(String, ForeignKey("carts.cart_id"))
    product_id = Column(String, ForeignKey("products.product_id"))
    quantity = Column(Integer)

    cart = relationship("Cart", back_populates="items")
    product = relationship("Product")

class Payment(Base):
    __tablename__ = "payments"
    payment_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = Column(String, ForeignKey("orders.order_id"))
    payment_method = Column(String)
    status = Column(String) # pending, success, failed
    amount = Column(Float)
    payment_date = Column(DateTime, default=datetime.utcnow)

class Shipment(Base):
    __tablename__ = "shipments"
    shipment_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    order_id = Column(String, ForeignKey("orders.order_id"))
    carrier = Column(String)
    tracking_number = Column(String)
    status = Column(String) # created, shipped, delivered
    delivery_date = Column(DateTime, nullable=True)