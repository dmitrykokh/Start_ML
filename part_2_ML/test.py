import app_2
from fastapi.testclient import TestClient
from datetime import datetime

client = TestClient(app_2.app)

user_id = 1234
time = datetime(2021, 12, 21)

try:
    r = client.get(
        f'/post/recommendations/',
        params={'id': user_id, 'time': time, 'limit': 5},
    )
except Exception as e:
    raise ValueError(f'ошибка при выполнении запроса')

print(r.json())
