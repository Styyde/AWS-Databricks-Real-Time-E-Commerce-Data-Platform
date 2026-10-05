from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas

router = APIRouter(prefix="/inventory", tags=["Inventory"])

@router.post("/")
def update_inventory(inventory: schemas.InventoryUpdate, db: Session = Depends(get_db)):
    # 1. Vérifier que le produit existe
    product = db.query(models.Product).filter(models.Product.product_id == inventory.product_id).first()
    if not product:
        raise HTTPException(404, f"Product {inventory.product_id} not found")
    
    # 2. Vérifier que l'entrepôt existe
    warehouse = db.query(models.Warehouse).filter(models.Warehouse.id == inventory.warehouse_id).first()
    if not warehouse:
        raise HTTPException(404, f"Warehouse {inventory.warehouse_id} not found")

    # 3. Chercher la ligne d'inventaire existante
    inv = db.query(models.Inventory).filter(
        models.Inventory.product_id == inventory.product_id,
        models.Inventory.warehouse_id == inventory.warehouse_id
    ).first()

    # 4. Si elle n'existe pas, on la crée. Sinon, on ajoute la quantité.
    if not inv:
        inv = models.Inventory(
            product_id=inventory.product_id,
            warehouse_id=inventory.warehouse_id,
            quantity=inventory.quantity
        )
        db.add(inv)
    else:
        inv.quantity += inventory.quantity

    db.commit()
    db.refresh(inv)
    return {"message": "Inventory updated", "new_quantity": inv.quantity}