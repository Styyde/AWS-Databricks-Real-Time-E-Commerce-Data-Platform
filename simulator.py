import asyncio
import httpx
from faker import Faker
import random

fake = Faker()
API_URL = "http://localhost:8000"

# --- Génération des données de base ---
async def generate_warehouses(client):
    """Crée quelques entrepôts"""
    cities = ["Casablanca", "Rabat", "Marrakech", "Tanger"]
    warehouse_ids = []
    for city in cities:
        data = {"name": f"Warehouse {city}", "city": city}
        resp = await client.post(f"{API_URL}/warehouses/", json=data)
        if resp.status_code == 200:
            warehouse_ids.append(resp.json()["id"])
    return warehouse_ids

async def generate_products(client, warehouse_ids):
    """Crée 5000 produits et associe des stocks à plusieurs entrepôts"""
    product_ids = []
    for _ in range(5000):
        prod_data = {
            "name": fake.catch_phrase(),
            "category": random.choice(["Electronics", "Clothing", "Home", "Books"]),
            "brand": fake.company(),
            "price": round(random.uniform(5.0, 2000.0), 2)
        }
        resp = await client.post(f"{API_URL}/products/", json=prod_data)
        if resp.status_code == 200:
            product_id = resp.json()["product_id"]
            product_ids.append(product_id)
            # Attribuer du stock à 1 ou 2 entrepôts aléatoires
            for _ in range(random.randint(1, 2)):
                wh = random.choice(warehouse_ids)
                inv_data = {
                    "product_id": product_id,
                    "warehouse_id": wh,
                    "quantity": random.randint(10, 200)
                }
                await client.post(f"{API_URL}/inventory/", json=inv_data)
    return product_ids

async def generate_customers(client):
    """Crée 100 000 clients"""
    customer_ids = []
    for _ in range(100000):
        cust_data = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "country": fake.country(),
            "city": fake.city()
        }
        resp = await client.post(f"{API_URL}/customers/", json=cust_data)
        if resp.status_code == 200:
            customer_ids.append(resp.json()["customer_id"])
        # petite pause tous les 10 pour ne pas surcharger l'API
        if _ % 10 == 0:
            await asyncio.sleep(0.01)
    return customer_ids

# --- Simulation d'un parcours d'achat complet ---
async def simulate_shopping_flow(client, customer_ids, product_ids):
    """Simule un client : panier -> ajout articles -> checkout -> paiement -> expédition"""
    customer_id = random.choice(customer_ids)
    
    # 1. Créer un panier
    cart_resp = await client.post(f"{API_URL}/carts/", json={"customer_id": customer_id})
    if cart_resp.status_code != 200:
        return
    cart_id = cart_resp.json()["cart_id"]
    
    # 2. Ajouter 1 à 4 produits au panier
    nb_items = random.randint(1, 4)
    selected_products = random.sample(product_ids, min(nb_items, len(product_ids)))
    for prod_id in selected_products:
        qty = random.randint(1, 3)
        await client.post(
            f"{API_URL}/carts/{cart_id}/items",
            json={"product_id": prod_id, "quantity": qty}
        )
    
    # 3. Checkout (transformer le panier en commande)
    checkout_resp = await client.post(
        f"{API_URL}/carts/{cart_id}/checkout",
        params={"payment_method": random.choice(["card", "paypal", "cash"])}
    )
    if checkout_resp.status_code != 200:
        # Abandon (simuler un abandon de panier)
        return
    order = checkout_resp.json()
    order_id = order["order_id"]
    total = order["total_amount"]
    
    # 4. Paiement
    payment_method = random.choice(["card", "paypal", "cash"])
    payment_resp = await client.post(
        f"{API_URL}/payments/",
        json={"order_id": order_id, "payment_method": payment_method, "amount": total}
    )
    if payment_resp.status_code != 200:
        return
    payment = payment_resp.json()
    if payment["status"] != "success":
        return
    
    # 5. Création de l'expédition
    ship_resp = await client.post(f"{API_URL}/shipments/", json={"order_id": order_id})
    if ship_resp.status_code != 200:
        return
    shipment = ship_resp.json()
    shipment_id = shipment["shipment_id"]
    
    # 6. Simuler la livraison (après un délai)
    await asyncio.sleep(random.uniform(0.1, 0.5))
    await client.patch(f"{API_URL}/shipments/{shipment_id}/deliver")

# --- Orchestrateur principal ---
async def main():
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("🌱 Génération des entrepôts...")
        warehouse_ids = await generate_warehouses(client)
        print("🌱 Génération des produits et inventaires...")
        product_ids = await generate_products(client, warehouse_ids)
        print("🌱 Génération des clients...")
        customer_ids = await generate_customers(client)
        
        print("🚀 Lancement des simulations d'achat (50 000 commandes)...")
        total_commands = 50000
        batch_size = 100
        for batch_start in range(0, total_commands, batch_size):
            tasks = [simulate_shopping_flow(client, customer_ids, product_ids) for _ in range(batch_size)]
            await asyncio.gather(*tasks)
            print(f"✅ Batch {batch_start//batch_size + 1}/{total_commands//batch_size} terminé")
        print("🎉 Simulation terminée !")

if __name__ == "__main__":
    asyncio.run(main())