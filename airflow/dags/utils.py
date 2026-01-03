import logging


class CallbackIOWrapper:
    def __init__(self, stream, callback):
        self.stream = stream
        self.callback = callback
    def read(self, n):
        chunk = self.stream.read(n)
        self.callback(len(chunk))
        return chunk
    
class ProgressPercentage(CallbackIOWrapper):
    def __init__(self, total_size, stream):
        self._total = total_size
        self._cursor = 0
        super().__init__(stream, self._logging_processed)
        
    def _logging_processed(self, chunk_size):
        self._cursor += chunk_size
        logging.info(f"Processed {self._cursor} of {self._total} bytes ({(self._cursor / self._total) * 100:.2f}%)")