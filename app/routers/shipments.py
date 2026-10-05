from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas
import random
from datetime import datetime

router = APIRouter(prefix="/shipments", tags=["Shipments"])

@router.post("/", response_model=schemas.ShipmentOut)
def create_shipment(shipment: schemas.ShipmentCreate, db: Session = Depends(get_db)):
    carriers = ["DHL", "FedEx", "UPS", "Aramex"]
    tracking = f"TRK{random.randint(100000, 999999)}"
    db_shipment = models.Shipment(
        order_id=shipment.order_id,
        carrier=random.choice(carriers),
        tracking_number=tracking,
        status="created"
    )
    db.add(db_shipment)
    db.commit()
    db.refresh(db_shipment)
    return db_shipment

@router.patch("/{shipment_id}/deliver")
def mark_delivered(shipment_id: str, db: Session = Depends(get_db)):
    shipment = db.query(models.Shipment).filter(models.Shipment.shipment_id == shipment_id).first()
    if not shipment:
        raise HTTPException(404, "Shipment not found")
    shipment.status = "delivered"
    shipment.delivery_date = datetime.utcnow()
    db.commit()
    return {"status": "delivered"}