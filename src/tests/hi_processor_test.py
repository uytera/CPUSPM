import asyncio
from io import BytesIO

from PIL import Image

from core.worker_manager import WorkerProcessManager, CPUCommands

images = [
    '.\\test_data\\1.jpg',
    '.\\test_data\\2.jpg',
    '.\\test_data\\3.jpg',
    '.\\test_data\\4.jpg',
]


async def async_main():
    WorkerProcessManager.prepare_start_method()
    wpm = WorkerProcessManager(2)
    try:
        cpc = CPUCommands(wpm)
        async with cpc.heatmap_image_accumulator(1024, 1024, "jpeg", "wow") as work_function:

            for image_path in images:
                with open(image_path, 'rb') as image_bytes:
                    img_bytes = image_bytes.read()
                    result = await work_function(img_bytes)

                    buffer = BytesIO(result)
                    try:
                        Image.open(buffer).show()
                    finally:
                        buffer.close()
    finally:
        wpm.stop_processes()

if __name__ == "__main__":
    asyncio.run(async_main())
