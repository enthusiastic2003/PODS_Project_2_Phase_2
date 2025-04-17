def test_concurrent_order_cancel_vs_delivery_tracked():
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

    user_id = 606
    product_id = 101
    num_threads = 20
    known_initial_stock = 10

    # Clean up users
    requests.delete("http://localhost:8080/users")

    # Setup
    post_user(user_id, "frank", "frank@example.com")
    put_wallet(user_id, "credit", 10000000)

    # Get initial stock
    initial_resp = get_product(product_id)
    initial_stock = initial_resp.json()["stock_quantity"]
    if initial_stock != known_initial_stock:
        print_fail_message(f"Expected {known_initial_stock} stock but found {initial_stock} — please reset the product stock.")
        return

    # Counters
    delivered_count = 0
    canceled_count = 0
    failed_cancel_after_delivery = 0
    lock = Lock()

    def order_deliver_or_cancel():
        nonlocal delivered_count, canceled_count, failed_cancel_after_delivery
        resp = post_order(user_id, [{"product_id": product_id, "quantity": 1}])
        if resp.status_code != 201:
            print_fail_message("Order failed")
            return

        order_id = resp.json()["order_id"]
        print(f"Order placed: {order_id}")

        is_delivered = random.choice([True, False])
        time.sleep(random.uniform(0.01, 0.2))

        if is_delivered:
            resp_deliver = putOrder(order_id)
            print(f"Delivered order {order_id}, status: {resp_deliver.status_code}")
            cancel_resp = delete_order(order_id)

            if cancel_resp.status_code in (400, 409):
                with lock:
                    delivered_count += 1
                print_pass_message(f"Cancel blocked after delivery (order {order_id})")
            else:
                with lock:
                    failed_cancel_after_delivery += 1
                print_fail_message(f"Cancelled after delivery! Order {order_id}")
        else:
            cancel_resp = delete_order(order_id)
            print(f"Cancelled order {order_id}, status: {cancel_resp.status_code}")
            if cancel_resp.status_code == 200:
                with lock:
                    canceled_count += 1

    # Run threads
    with ThreadPoolExecutor(max_workers=num_threads * 2) as executor:
        futures = [executor.submit(order_deliver_or_cancel) for _ in range(num_threads)]

    for f in as_completed(futures):
        f.result()

    # Final stock check
    final_resp = get_product(product_id)
    final_stock = final_resp.json()["stock_quantity"]

    # ✅ FIXED: Only delivered orders reduce stock
    expected_stock = known_initial_stock - delivered_count

    print("\n=== FINAL STOCK ASSERTION ===")
    print(f"Initial stock:      {known_initial_stock}")
    print(f"Delivered orders:   {delivered_count}")
    print(f"Canceled orders:    {canceled_count}")
    print(f"Expected stock:     {expected_stock}")
    print(f"Actual stock:       {final_stock}")

    if final_stock == expected_stock:
        print_pass_message("✅ Stock is consistent with delivered orders.")
    else:
        print_fail_message("❌ Stock mismatch! Expected doesn't match actual.")

    if delivered_count > known_initial_stock:
        print_fail_message("🚨 Too many delivered orders! Stock over-committed!")
    else:
        print_pass_message("✅ Delivered count is within available stock.")

    if failed_cancel_after_delivery > 0:
        print_fail_message(f"{failed_cancel_after_delivery} cancellations succeeded after delivery! 🚫")

if __name__ == "__main__":
    test_concurrent_order_cancel_vs_delivery_tracked()
