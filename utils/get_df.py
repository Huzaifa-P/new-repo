import awswrangler as wr

def get_df(bucket_one, key):
    return wr.s3.read_parquet(
            f"s3://{bucket_one}/{key}")