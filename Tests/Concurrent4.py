import requests

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
    test_post_order,
    delete_order
)


try:
    response = requests.delete("http://localhost:8080/users")
    print("Status Code:", response.status_code)
    print("Response Body:", response.text)
except requests.exceptions.RequestException as e:
    print("An error occurred:", e)

resp = post_user(101, "alice", "alice@concurrency.com")
resp_pw = put_wallet(101,"credit" ,20000000)
resp = post_user(102, "blice", "blice@concurrency.com")
resp_pw = put_wallet(102,"credit" ,20000000)

order_id_list = []

resp_po = post_order(101, [{"product_id": 101, "quantity": 2},{"product_id": 102,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(101, [{"product_id": 101, "quantity": 2},{"product_id": 102,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(101, [{"product_id": 101, "quantity": 2},{"product_id": 102,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(101, [{"product_id": 103, "quantity": 2},{"product_id": 104,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(101, [{"product_id": 103, "quantity": 2},{"product_id": 104,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(102, [{"product_id": 105, "quantity": 2},{"product_id": 106,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(102, [{"product_id": 105, "quantity": 2},{"product_id": 106,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(102, [{"product_id": 105, "quantity": 2},{"product_id": 106,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(102, [{"product_id": 111, "quantity": 2},{"product_id": 120,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])
resp_po = post_order(102, [{"product_id": 111, "quantity": 2},{"product_id": 120,"quantity": 1}])
order_id_list.append(resp_po.json()['order_id'])

print(order_id_list)

for order_id in order_id_list:
    resp = delete_order(order_id)
    if resp.status_code == 200:
        print(f"Order {order_id} deleted successfully.")
    else:
        print(f"Failed to delete order {order_id}. Status code: {resp.status_code}")
    resp = delete_order(order_id)
    if resp.status_code == 200:
        print(f"Order {order_id} deleted successfully.")
    else:
        print(f"Failed to delete order {order_id}. Status code: {resp.status_code}")


thread_count = 10
threads = []
successful_deletions = 0
def delete_order_thread(order_id, attempts=5):

    for _ in range(attempts):
        resp = delete_order(order_id)
        print(resp)
        resp = delete_order(order_id-5)
        print(resp)

        

for i in range(thread_count):
    t = Thread(target=delete_order_thread, kwargs={
        "order_id": order_id
    })
    threads.append(t)
    t.start()

for t in threads:
    t.join()