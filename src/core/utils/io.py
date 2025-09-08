import os
from io import IOBase


class ReadNoCopyIO(IOBase):
    def __init__(self, raw_bytes: bytes):
        self.buf = memoryview(raw_bytes)
        self.pos: int = 0
        self.buf_length = len(self.buf)

    def close(self):
        self.buf.release()

    def seek(self, offset: int, whence: int = os.SEEK_SET):
        new_offset = 0
        match whence:
            case os.SEEK_SET:
                new_offset = offset
            case os.SEEK_CUR:
                new_offset = self.pos + offset
            case os.SEEK_END:
                new_offset = self.buf_length - 1 + offset

        if new_offset < 0 or new_offset > self.buf_length:
            raise Exception("Invalid offset passed")
        else:
            self.pos = new_offset

    def read(self, chunk_size: int) -> memoryview:
        start = self.pos
        end = start + chunk_size
        return self.buf[start:end]
