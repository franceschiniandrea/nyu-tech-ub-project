import pandas as pd
from datetime import datetime
from google.cloud import storage # type: ignore
import os
import logging

logging.basicConfig(level=logging.INFO)


class GCSFallbackWriter:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def _make_paths(self, symbol: str, data_type: str):
        now = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        local_path = f"/tmp/{data_type}_{symbol}_{now}.parquet"
        gcs_path = f"{data_type}/{symbol}/{now}.parquet"
        return local_path, gcs_path

    def save_and_upload(self, symbol: str, data_type: str, columns: list[str], batch_data: list[tuple]):
        try:
            if not batch_data:
                return

            # Save using only the columns provided by QueueProcessor
            df = pd.DataFrame(batch_data, columns=columns)
            local_path, gcs_path = self._make_paths(symbol, data_type)

            df.to_parquet(local_path, engine="pyarrow")

            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)

            logging.warning(f"[GCS ✅] Uploaded fallback: gs://{self.bucket.name}/{gcs_path}")
            os.remove(local_path)

        except Exception as e:
            logging.error(f"[GCS ❌] Failed to write GCS fallback for {symbol}: {e}")
