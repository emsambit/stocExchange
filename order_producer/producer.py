'''
### 3. **Order Producer** (order_producer/producer.py)

```python
'''
import json
from kafka import KafkaProducer
import time

# Load instruments
with open('instruments.json') as f:
    instruments = json.load(f)

producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate traders placing orders
orders = [
    {"trader_id": 1, "instrument": "AAPL", "type": "buy", "price": 150, "quantity": 10, "timestamp": time.time()},
    {"trader_id": 2, "instrument": "AAPL", "type": "sell", "price": 150, "quantity": 5, "timestamp": time.time()},
    {"trader_id": 3, "instrument": "GOOGL", "type": "buy", "price": 2800, "quantity": 2, "timestamp": time.time()}
]

for i in range(100000):
    for order in orders:
        producer.send('orders_input', order)
        print(f"Order sent: {order}")
        time.sleep(1)
    time.sleep(5)
producer.flush()



