# Hệ thống đơn hàng với Solace (Python)

## Chạy
1. `docker compose up -d` rồi đợi 1-2 phút (xem `docker logs -f solace`).
2. `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\Scripts\activate`)
3. `pip install -r requirements.txt`
4. Mở 2 terminal, chạy mỗi consumer một cái:
   - `python order_processor.py`
   - `python notification_service.py`
5. Terminal thứ 3: `python order_publisher.py`

Broker Manager: http://localhost:8080 (admin / admin) -> Queues để xem message.

## Topic
- `shop/orders/created`
- `shop/orders/cancelled`

## Queue
- `orders-queue` <- `shop/orders/>` (nhận cả created và cancelled)
- `notifications-queue` <- `shop/orders/created`

cd C:\Learning_Path\Solace_Demo
.venv\Scripts\Activate.ps1

python order_processor.py 
python notification_service.py
python order_publisher.py