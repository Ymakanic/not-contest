import requests
import time

BASE_URL = "http://localhost:8080"

def test_normal_flow():
    print("=== Testing Normal Flow ===")
    
    # Reserve an item
    reserve_res = requests.post(
        f"{BASE_URL}/checkout",
        params={"user_id": "user1", "id": "item100"}
    )
    assert reserve_res.status_code == 200
    code = reserve_res.json()["code"]
    print(f"Reservation successful: {code}")
    
    time.sleep(1)  # Simulate user delay
    
    # Purchase the item
    purchase_res = requests.post(
        f"{BASE_URL}/purchase",
        params={"code": code}
    )
    assert purchase_res.status_code == 200
    print(f"Purchase successful: {purchase_res.json()['message']}")
    
    # Check status
    status_res = requests.get(f"{BASE_URL}/status")
    assert status_res.status_code == 200
    status_data = status_res.json()
    print("Current status:")
    print(f"  Successful checkouts: {status_data['successful_checkouts']}")
    print(f"  Successful purchases: {status_data['successful_purchases']}")
    print("Test passed!\n")

if __name__ == "__main__":
    test_normal_flow()
