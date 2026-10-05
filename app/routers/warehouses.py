
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas

router = APIRouter(prefix="/warehouses", tags=["Warehouses"])

@router.post("/", response_model=schemas.WarehouseOut)
def create_warehouse(warehouse: schemas.WarehouseCreate, db: Session = Depends(get_db)):
    db_warehouse = models.Warehouse(name=warehouse.name, city=warehouse.city)
    db.add(db_warehouse)
    db.commit()
    db.refresh(db_warehouse)
    return db_warehouse