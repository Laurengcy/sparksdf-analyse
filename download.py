def download_folder_from_s3(aws_access_key_id_, aws_secret_access_key_, aws_session_token_,
                            bucket_addr, folder_prefix, desti_folder_path):
    S3_RESOURCE = boto3.resource(
    's3',
    # Hard coded strings as credentials, not recommended.
    aws_access_key_id = aws_access_key_id_,
    aws_secret_access_key = aws_secret_access_key_, 
    aws_session_token = aws_session_token_
)
    
    bucket = S3_RESOURCE.Bucket(bucket_addr) 
    
    for obj in bucket.objects.filter(Prefix = folder_prefix):
        
        if not os.path.exists(desti_folder_path):
            os.makedirs(desti_folder_path)
            
        bucket.download_file(obj.key, f'{desti_folder_path}/{os.path.basename(os.path.normpath(obj.key))}')