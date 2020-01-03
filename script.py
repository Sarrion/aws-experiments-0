def load(path, bucket_name = 'bme-bucket'):
    import io
    import pickle
    import boto3
        
    s3_client = boto3.client('s3')
    array = io.BytesIO()
    s3_client.download_fileobj(bucket_name, path, array)
    array.seek(0)
    return pickle.load(array)