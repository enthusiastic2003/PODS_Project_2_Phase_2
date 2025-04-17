def test_random_cancel_or_deliver_and_checksum():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    import requests
    import random
    from threading import Lock

    from marketplace import post_order, delete_order, get_product, test_get_product_stock
    from user import post_user
    from wallet import put_wallet
    from utils import print_fail_message, print_pass_message

    def putOrder(order_id):
        return requests.put(f"http://localhost:8081/orders/{order_id}", json={"order_id": order_id, "status": "DELIVERED"})

    user_id = 909
    product_id = 101
    num_threads = 20
    known_initial_stock = 10

    # Clean up users
    requests.delete("http://localhost:8080/users")

    # Setup
    post_user(user_id, "randomzoe", "randomzoe@example.com")
    put_wallet(user_id, "credit", 10000000)

    # Check initial stock
    initial_resp = get_product(product_id)
    initial_stock = initial_resp.json()["stock_quantity"]
    if initial_stock != known_initial_stock:
        print_fail_message(f"Expected {known_initial_stock} stock but found {initial_stock} — please reset the product stock.")
        return

    # Tracking
    canceled_orders = []
    delivered_orders = []
    failed_deliveries = []
    failed_cancel_after_delivery = []
    lock = Lock()

    def random_cancel_or_deliver():
        resp = post_order(user_id, [{"product_id": product_id, "quantity": 1}])
        if resp.status_code != 201:
            print_fail_message("❌ Order creation failed.")
            return

        order_id = resp.json()["order_id"]
        print(f"➡️ Order placed: {order_id}")

        action = random.choice(["cancel", "deliver"])  # Random action
        time.sleep(random.uniform(0.01, 0.1))  # Simulate delay

        if action == "cancel":
            cancel_resp = delete_order(order_id)
            if cancel_resp.status_code == 200:
                with lock:
                    canceled_orders.append(order_id)
                print_pass_message(f"✅ Order {order_id} canceled successfully")
            else:
                print_fail_message(f"❌ Cancel failed for order {order_id}")
        else:
            deliver_resp = putOrder(order_id)
            if deliver_resp.status_code == 200:
                with lock:
                    delivered_orders.append(order_id)
                print_pass_message(f"✅ Order {order_id} delivered successfully")
            else:
                with lock:
                    failed_deliveries.append(order_id)
                print_fail_message(f"❌ Failed to deliver order {order_id}")

            # Try cancel after deliver to see if it's blocked
            cancel_resp = delete_order(order_id)
            if cancel_resp.status_code == 200:
                with lock:
                    failed_cancel_after_delivery.append(order_id)
                print_fail_message(f"🚫 Order {order_id} canceled AFTER delivery!")
            else:
                print_pass_message(f"✅ Cancel blocked after delivery (order {order_id})")

    # Run threads
    with ThreadPoolExecutor(max_workers=num_threads * 2) as executor:
        futures = [executor.submit(random_cancel_or_deliver) for _ in range(num_threads)]

    for f in as_completed(futures):
        f.result()

    # Final stock check
    final_resp = get_product(product_id)
    final_stock = final_resp.json()["stock_quantity"]
    expected_stock = known_initial_stock - len(delivered_orders)

    print("\n=== ✅ FINAL CHECKSUM REPORT ===")
    print(f"Initial stock:                     {known_initial_stock}")
    print(f"Delivered orders:                 {len(delivered_orders)} → {delivered_orders}")
    print(f"Canceled orders:                  {len(canceled_orders)} → {canceled_orders}")
    print(f"Failed deliveries:                {len(failed_deliveries)}")
    print(f"Cancels after delivery (invalid): {len(failed_cancel_after_delivery)} → {failed_cancel_after_delivery}")
    print(f"Expected final stock:             {expected_stock}")
    print(f"Actual final stock:               {final_stock}")

    if final_stock == expected_stock:
        print_pass_message("✅ Stock matches expected based on actual deliveries.")
    else:
        print_fail_message("❌ Stock mismatch! Please investigate logic or race conditions.")

    if len(failed_cancel_after_delivery) > 0:
        print_fail_message("🚨 Some orders were canceled after delivery — this should NOT happen!")

if __name__ == "__main__":
    test_random_cancel_or_deliver_and_checksum()

