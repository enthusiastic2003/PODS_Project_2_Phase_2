import threading
import requests
import time

BASE_URL = "http://localhost:8081"  # Replace with your actual API endpoint

import wallet

user_id = 1001
product_id = 101

prod = requests.get(f"{BASE_URL}/products/{product_id}").json()

initial_stock = prod["stock_quantity"]
product_price = prod["price"]
thread_count = initial_stock + 5  # More than available stock to test rollback
wallet_amt = 10000000
USR_URL = "http://localhost:8080/users"


def add_user(id,  name, email):
    user_data = {
        "id": id,
        "name": name,
        "email": email
    }

    response = requests.post(f"{USR_URL}", json=user_data)

    print(f"Response: {response.status_code}, {response.text}")
    return response.status_code



def place_order():
    order_data = {
        "user_id": user_id,
        "items": [{"product_id": product_id, "quantity": 1}]
    }

    response = requests.post(f"{BASE_URL}/orders", json=order_data)

    print(f"Response: {response.status_code}, {response.text}")
    return response.status_code

def get_product_details(id):

    response = requests.get(f"{BASE_URL}/products/{id}")

    return response.json() if response.status_code !=400 else None

def test_concurrent_orders():
    threads = []
    results = []

    initial_stock_amount = get_product_details(product_id).get('stock_quantity')



    for _ in range(thread_count):
        thread = threading.Thread(target=lambda: results.append(place_order()))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Analyze results
    success_count = results.count(201)  # HTTP 201 = Created
    failure_count = len(results) - success_count

    print(f"Success Count: {success_count}")
    print(f"Failure Count: {failure_count}")

    # Expected outcome: Successes should not exceed initial stock
    stock_after_buy = get_product_details(product_id)["stock_quantity"]

    total_price = 0.9*product_price + (initial_stock-1)*product_price

    assert wallet_amt - total_price == wallet.get_wallet(user_id).json()['balance'], "Wallet was not updated correctly!"

    assert stock_after_buy == initial_stock_amount - success_count, "Stock was not updated correctly!"
    assert success_count <= initial_stock, "More orders were placed than available stock!"
    assert stock_after_buy == 0, "All must have been deducted"
    assert success_count + failure_count == thread_count, "All threads must have completed"


if __name__ == "__main__":
    resp = add_user(user_id, "John Doe", "etc@gmail.com")
    if resp != 201:
        print("Failed to add user")
        exit(1)
    
    wallet.put_wallet(user_id, 'credit', wallet_amt)
    if wallet.get_wallet(user_id).status_code != 200:
        print("Failed to credit wallet")
        exit(1)
    else:
            print("Wallet credited successfully")
    test_concurrent_orders()

    print("All tests passed!")