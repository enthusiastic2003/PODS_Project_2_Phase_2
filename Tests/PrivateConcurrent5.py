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

def putOrder(order_id):
    return requests.put(f"http://localhost:8081/orders/{order_id}", json={"order_id": order_id, "status": "DELIVERED"})

 
# cancle then try to deliver
# delete all users
requests.delete("http://localhost:8080/users")
# put an user
post_user(101, "alice", "alice@gmail.com")
# put money in wallet
put_wallet(101,"credit" ,20000000)
# put an order
resp_postorder = post_order(101, [{"product_id": 101, "quantity": 2},{"product_id": 102,"quantity": 1}])
# cancle the order
order_id = resp_postorder.json()['order_id']
delete_order(order_id)
# try to deliver the order
response = putOrder(order_id)
print("Status Code:", response.status_code)
print("Response Body:", response.text)
check_response_status_code(response, 400)  # Assuming the order cannot be delivered after cancellation