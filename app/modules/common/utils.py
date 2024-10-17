from app.core.dependencies import s3_client
from io import BytesIO
import pandas as pd

def read_s3_file(bucket: str, file_path: str):
    response = s3_client.get_object(Bucket=bucket, Key=file_path)
    parquet_content = response['Body'].read()
    df = pd.read_parquet(BytesIO(parquet_content))
    return df