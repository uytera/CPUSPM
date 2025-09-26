import asyncio
from io import BytesIO

from PIL import Image

from core.worker.worker_manager import WorkerProcessManager, CPUCommands

images = [
    '.\\test_data\\1.jpg'
]


async def async_main():
    WorkerProcessManager.prepare_start_method()
    wpm = WorkerProcessManager(2)

    try:
        cpc = CPUCommands(wpm)

        with open(images[0], 'rb') as image_bytes:
            img_bytes = image_bytes.read()
            image = await cpc.image_to_grayscale(img_bytes)

        buffer = BytesIO(image)
        try:
            Image.open(buffer).show()
        finally:
            buffer.close()
    finally:
        wpm.stop_processes()

if __name__ == "__main__":
    asyncio.run(async_main())
