import sys
import random
from threading import Thread
import time

from user import post_user, get_user, test_get_user
from wallet import put_wallet, get_wallet, test_get_wallet
from marketplace import (
    post_order,
    get_product,
    delete_order,
    test_get_product_stock,
    test_post_order,
    test_delete_order
)
from utils import check_response_status_code, print_fail_message, print_pass_message

def place_orders_for_single_user(user_id, product_id, quantity_per_order, num_orders):
    """
    Place multiple orders for a single user.
    Returns a list of successful order IDs.
    """
    successful_orders = []
    
    for _ in range(num_orders):
        resp = post_order(user_id, [{"product_id": product_id, "quantity": quantity_per_order}])
        
        if resp.status_code == 201:
            if test_post_order(
                user_id,
                items=[{"product_id": product_id, "quantity": quantity_per_order}],
                response=resp,
                expect_success=True
            ):
                # Extract order_id and add to successful list
                order_id = resp.json().get('order_id')
                successful_orders.append(order_id)
                print_pass_message(f"Order {order_id} successfully placed")
            else:
                print_fail_message("test_post_order failed on success scenario")
        elif resp.status_code == 400:
            # Expected failure (insufficient stock or balance)
            test_post_order(
                user_id,
                items=[{"product_id": product_id, "quantity": quantity_per_order}],
                response=resp,
                expect_success=False
            )
            print_pass_message("Order failed as expected (likely insufficient stock or balance)")
        else:
            print_fail_message(f"Unexpected status code {resp.status_code} for POST /orders")
    
    return successful_orders

def main():
    try:
        # 1) Create multiple users
        num_users = 3
        user_ids = []
        
        for i in range(num_users):
            user_id = 3000 + i
            user_name = f"Tester{i}"
            user_email = f"tester{i}@example.com"
            
            resp = post_user(user_id, user_name, user_email)
            if not check_response_status_code(resp, 201):
                print_fail_message(f"Failed to create user {user_id}")
                return False
            
            # 2) Credit each user's wallet with random amounts
            wallet_amount = random.randint(10000, 20000)
            resp = put_wallet(user_id, "credit", wallet_amount)
            if not check_response_status_code(resp, 200):
                print_fail_message(f"Failed to credit wallet for user {user_id}")
                return False
            
            user_ids.append(user_id)
            print_pass_message(f"Created user {user_id} with wallet balance {wallet_amount}")
        
        # 3) Check product's initial stock
        product_id = 102  # Using a different product from the original test
        resp = get_product(product_id)
        if not check_response_status_code(resp, 200):
            print_fail_message(f"Failed to get product {product_id}")
            return False
        
        initial_stock = resp.json().get('stock_quantity')
        print_pass_message(f"Initial stock for product {product_id}: {initial_stock}")
        
        if initial_stock <= 0:
            print_fail_message("Product has no stock, cannot proceed with test")
            return False
        
        # 4) Start threads for each user to place orders concurrently
        threads = []
        all_successful_orders = []
        
        # Each user will try to place multiple orders
        orders_per_user = 2
        quantity_per_order = 1
        
        for user_id in user_ids:
            def user_order_thread(uid=user_id):
                orders = place_orders_for_single_user(
                    uid, 
                    product_id, 
                    quantity_per_order, 
                    orders_per_user
                )
                all_successful_orders.extend(orders)
            
            t = Thread(target=user_order_thread)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # 5) Verify final stock
        resp = get_product(product_id)
        if not check_response_status_code(resp, 200):
            print_fail_message(f"Failed to get product {product_id} after orders")
            return False
        
        final_stock = resp.json().get('stock_quantity')
        total_successful_quantity = len(all_successful_orders) * quantity_per_order
        expected_final_stock = initial_stock - total_successful_quantity
        
        print_pass_message(f"Total successful orders: {len(all_successful_orders)}")
        print_pass_message(f"Total quantity ordered: {total_successful_quantity}")
        print_pass_message(f"Final stock: {final_stock}, Expected: {expected_final_stock}")
        
        if final_stock != expected_final_stock:
            print_fail_message(f"Stock mismatch: expected {expected_final_stock}, got {final_stock}")
            return False
        
        # 6) Try to cancel some orders and verify status
        if len(all_successful_orders) > 0:
            # Try to cancel half of the orders
            orders_to_cancel = all_successful_orders[:len(all_successful_orders)//2]
            
            for order_id in orders_to_cancel:
                resp = delete_order(order_id)
                if not test_delete_order(order_id, resp, can_cancel=True):
                    print_fail_message(f"Failed to cancel order {order_id}")
                else:
                    print_pass_message(f"Successfully cancelled order {order_id}")
        
        # 7) Check final wallet balances
        for user_id in user_ids:
            resp = get_wallet(user_id)
            if not check_response_status_code(resp, 200):
                print_fail_message(f"Failed to get wallet for user {user_id}")
                return False
            
            balance = resp.json().get('balance')
            print_pass_message(f"Final balance for user {user_id}: {balance}")
        
        print_pass_message("Multiple user concurrent order test completed successfully")
        return True
        
    except Exception as e:
        print_fail_message(f"Test crashed: {e}")
        return False

if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1)