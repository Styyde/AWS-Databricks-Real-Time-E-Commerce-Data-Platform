from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas

router = APIRouter(prefix="/orders", tags=["Orders"])

@router.post("/", response_model=schemas.OrderOut)
def create_order(order: schemas.OrderCreate, db: Session = Depends(get_db)):
    try:
        total_amount = 0.0
        order_items_data = []

        for item in order.items:
            # 1. Vérifier que le produit existe
            product = db.query(models.Product).filter(models.Product.product_id == item.product_id).first()
            if not product:
                raise HTTPException(status_code=404, detail=f"Produit {item.product_id} introuvable")

            # 2. Vérifier le stock dans l'inventaire (on prend le premier entrepôt disponible)
            inventory = db.query(models.Inventory).filter(models.Inventory.product_id == item.product_id).first()
            if not inventory:
                raise HTTPException(status_code=400, detail=f"Aucun stock trouvé pour {product.name}")
            if inventory.quantity < item.quantity:
                raise HTTPException(status_code=400, detail=f"Stock insuffisant pour {product.name}")

            # 3. Décrémenter le stock
            inventory.quantity -= item.quantity
            total_amount += item.quantity * product.price
            
            order_items_data.append(models.OrderItem(
                product_id=item.product_id,
                quantity=item.quantity,
                unit_price=product.price
            ))

        # 4. Créer la commande
        db_order = models.Order(
            customer_id=order.customer_id,
            total_amount=total_amount,
            payment_method=order.payment_method,
            order_items=order_items_data
        )

        db.add(db_order)
        db.commit()
        db.refresh(db_order)

        return db_order

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))