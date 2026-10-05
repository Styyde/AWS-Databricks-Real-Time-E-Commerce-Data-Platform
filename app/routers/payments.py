from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app import models, schemas
import random

router = APIRouter(prefix="/payments", tags=["Payments"])

@router.post("/", response_model=schemas.PaymentOut)
def create_payment(payment: schemas.PaymentCreate, db: Session = Depends(get_db)):
    status = "success" if random.random() > 0.1 else "failed"  # 10% d'échec
    db_payment = models.Payment(
        order_id=payment.order_id,
        payment_method=payment.payment_method,
        amount=payment.amount,
        status=status
    )
    db.add(db_payment)
    db.commit()
    db.refresh(db_payment)
    return db_payment