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
    wpm = WorkerProcessManager(2)
    try:
        wpm.prepare_start_method()

        cpc = CPUCommands(wpm)
        async with cpc.average_image_accumulator(1024, 1024, "jpeg", "wow") as args_tuple:
            work_function, result_list = args_tuple

            for image_path in images:
                with open(image_path, 'rb') as image_bytes:
                    img_bytes = image_bytes.read()
                    await work_function(img_bytes)

        buffer = BytesIO(result_list[0])
        try:
            Image.open(buffer).show()
        finally:
            buffer.close()
    finally:
        wpm.stop_processes()

if __name__ == "__main__":
    asyncio.run(async_main())
