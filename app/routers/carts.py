from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas
from datetime import datetime

router = APIRouter(prefix="/carts", tags=["Carts"])

@router.post("/", response_model=schemas.CartOut)
def create_cart(cart: schemas.CartCreate, db: Session = Depends(get_db)):
    db_cart = models.Cart(customer_id=cart.customer_id)
    db.add(db_cart)
    db.commit()
    db.refresh(db_cart)
    return db_cart

@router.post("/{cart_id}/items", response_model=schemas.CartItemOut)
def add_item(cart_id: str, item: schemas.CartItemCreate, db: Session = Depends(get_db)):
    cart = db.query(models.Cart).filter(models.Cart.cart_id == cart_id).first()
    if not cart or cart.status != "active":
        raise HTTPException(400, "Cart not active")
    product = db.query(models.Product).filter(models.Product.product_id == item.product_id).first()
    if not product:
        raise HTTPException(404, "Product not found")
    # Vérifier le stock dans un entrepôt (simplifié)
    inventory = db.query(models.Inventory).filter(models.Inventory.product_id == item.product_id).first()
    if not inventory or inventory.quantity < item.quantity:
        raise HTTPException(400, "Insufficient stock")
    db_item = models.CartItem(cart_id=cart_id, product_id=item.product_id, quantity=item.quantity)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

@router.post("/{cart_id}/checkout", response_model=schemas.OrderOut)
def checkout(cart_id: str, payment_method: str, db: Session = Depends(get_db)):
    cart = db.query(models.Cart).filter(models.Cart.cart_id == cart_id).first()
    if not cart or cart.status != "active":
        raise HTTPException(400, "Cart not active")
    items = db.query(models.CartItem).filter(models.CartItem.cart_id == cart_id).all()
    if not items:
        raise HTTPException(400, "Cart empty")
    
    total = 0
    order_items = []
    for item in items:
        product = db.query(models.Product).filter(models.Product.product_id == item.product_id).first()
        if not product:
            raise HTTPException(404, f"Product {item.product_id} not found")
        inventory = db.query(models.Inventory).filter(models.Inventory.product_id == item.product_id).first()
        if not inventory or inventory.quantity < item.quantity:
            raise HTTPException(400, f"Insufficient stock for {product.name}")
        inventory.quantity -= item.quantity
        total += item.quantity * product.price
        order_items.append(models.OrderItem(product_id=item.product_id, quantity=item.quantity, unit_price=product.price))
    
    db_order = models.Order(customer_id=cart.customer_id, total_amount=total, payment_method=payment_method, order_items=order_items)
    db.add(db_order)
    cart.status = "converted"
    db.commit()
    db.refresh(db_order)
    return db_order