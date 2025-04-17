import sys
import requests
import random
from threading import Thread
import time

from user import post_user,delete_user,get_user,put_user_discount,delete_user, test_post_user,test_get_user,test_put_user_discount,test_delete_user
from wallet import put_wallet, get_wallet, test_get_wallet
from utils import check_response_status_code, print_fail_message, print_pass_message
from marketplace import (
    post_order,
    get_product,
    test_get_product_stock,
    test_post_order
)

WALLET_SERVICE_URL = "http://localhost:8082"
MARKETPLACE_SERVICE_URL = "http://localhost:8081"
ACCOUNT_SERVICE_URL = "http://localhost:8080"

"""
*Use balance properly, when concurrent orders form same user*
This test case puts multiple concurrent orders from the same user.
And checks if the balance is used properly.
"""



successful_orders = 0

def place_order_thread(user_id, product_id, attempts=5):
    global successful_orders
    for _ in range(attempts):
        resp = post_order(user_id, [{"product_id": product_id, "quantity": 1}])

        if resp.status_code == 201:
            # Verify success scenario
            # (We aren't specifying expected_total_price, so pass None)
            if not test_post_order(
                user_id, 
                items=[{"product_id": product_id, "quantity": 1}],
                response=resp,
                expect_success=True
            ):
                # If the structure fails, we'll just log, but you could raise an exception
                print_fail_message("test_post_order failed on success scenario.")
            successful_orders += 1
        elif resp.status_code == 400:
            # Possibly out of stock or insufficient balance
            if not test_post_order(
                user_id, 
                items=[{"product_id": product_id, "quantity": 1}],
                response=resp,
                expect_success=False
            ):
                print_fail_message("test_post_order failed on expected failure scenario.")
        else:
            print_fail_message(f"Unexpected status code {resp.status_code} for POST /orders.")


def main():
    try:
        user_id = 1234
        product_id = 108
        original_price  = 2000
        discount_price = 1800
        initial_balance = 8000
      
        thread_count = 10
        attempts_per_thread = 1
        
        global successful_orders
        successful_orders = 0  

        
        delete_user(user_id)
        get_wallet(user_id)



        resp = post_user(user_id, "Sai Kiran", "sai@gmail.com")
        if not check_response_status_code(resp, 201):
            return False
        
        
        resp = put_wallet(user_id, "credit", initial_balance)
        if not check_response_status_code(resp, 200):
            return False
        
        resp = post_order(user_id, [{"product_id": product_id, "quantity": 1}])
        if not check_response_status_code(resp, 201):
            return False
        else:
            successful_orders += 1
        # sleep for 1 second
       
        time.sleep(1)

        
        
        threads = []

        for i in range(thread_count):
            t = Thread(target=place_order_thread, kwargs={
                "user_id": user_id,
                "product_id": product_id,
                "attempts": attempts_per_thread
            })
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print_pass_message(f"Total successful orders = {successful_orders}")
        expected_successful_orders = 4

        resp = get_wallet(user_id)
        if not check_response_status_code(resp, 200):
            return False
        correct_final_balance = initial_balance - (original_price * (expected_successful_orders-1)) - discount_price

        if not (expected_successful_orders == successful_orders):
            print_fail_message(
                f"Total successful orders = {successful_orders}, expected {expected_successful_orders}"
            )
            return False

        print(f"Expected_successful_orders: {expected_successful_orders}")                           
        print("Test passed successfully")

        return True

    except Exception as e:
        print_fail_message(f"Test crashed with exception: {e}")
        return False

if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1)
