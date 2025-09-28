import json
import time
from io import BytesIO

import websocket
from PIL import Image

cpuspm_host = "192.168.122.175:8090"
url = f"ws://{cpuspm_host}/ws"
end_code = 5

images = [
    '.\\test_data\\1.jpg',
    '.\\test_data\\2.jpg',
    '.\\test_data\\3.jpg',
    '.\\test_data\\4.jpg',
]

ws = websocket.WebSocket()
ws.connect(url)

ws.send_text(json.dumps({
    "flow_type": 1,
    "context_data": {
        "width": 1024,
        "height": 1024,
        "img_format": "jpeg"
    }
}))

print(f"Recv transferred permitted: {ws.recv()}")

time.sleep(100)

for image_path in images:
    with open(image_path, 'rb') as image_bytes:
        print("Send image")
        ws.send_bytes(image_bytes.read())

ws.send_bytes(end_code.to_bytes(1, 'big'))

buffer = BytesIO(ws.recv())
try:
    Image.open(buffer).show()
finally:
    buffer.close()

print(f"Flow ended: {ws.recv()}")
ws.close()
