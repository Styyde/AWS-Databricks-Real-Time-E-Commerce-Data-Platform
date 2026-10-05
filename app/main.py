from fastapi import FastAPI
from app.database import engine
from app import models
# Importez les routers existants et les nouveaux
from app.routers import orders, customers, products, carts, payments, shipments, inventory, warehouses
# Crée toutes les tables (y compris les nouvelles) au démarrage
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="E-Commerce API", description="Simulation de transactions")

# Inclusion des routers
app.include_router(orders.router)
app.include_router(customers.router)
app.include_router(products.router)
app.include_router(carts.router)
app.include_router(payments.router)
app.include_router(shipments.router)
app.include_router(inventory.router)
app.include_router(warehouses.router)

@app.get("/")
def root():
    return {"message": "API de commerce en ligne active"}