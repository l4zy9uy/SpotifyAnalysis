import s3fs

fs = s3fs.S3FileSystem(anon=False)

with fs.open('s3://don-spotify-bucket/000CbwTZICdj6uprlrc1f1.pickle', 'rb') as f:
    data = f.read()
    print(data)
