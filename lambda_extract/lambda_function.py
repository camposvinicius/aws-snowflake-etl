import requests, io, tempfile, os, boto3
from zipfile import ZipFile

file_name = '2m-Sales-Records.zip'
bucket = "data-rawzone"
url = 'https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip'

def lambda_handler(event, context):

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0"
    }

    s3 = boto3.resource('s3')

    with tempfile.TemporaryDirectory() as temp_path:
        temp_dir = os.path.join(temp_path, 'temp')
        with open(temp_dir, 'wb') as f:
            req = requests.get(url, headers=headers)  
            f.write(req.content)
        
        s3.Bucket(bucket).upload_file(temp_dir, file_name)
    
        zip_obj = s3.Object(bucket_name=bucket, key=file_name)
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        
        z = ZipFile(buffer)
        for filename in z.namelist():
            s3.meta.client.upload_fileobj(
                z.open(filename),
                Bucket=bucket,
                Key='data/' + f'{filename}'
            )

    for file in s3.Bucket(bucket).objects.all():

        ## Removing the whitespaces in the filename

        if str(file).__contains__('data/'):
            OLD_NAME = file.key
            NEW = file.key.replace(" ", "_")

            try:
                s3.Object(bucket, NEW).copy_from(CopySource=f'{bucket}/{OLD_NAME}')
                s3.Object(bucket, OLD_NAME).delete()
            except:
                pass
            
        ## Moving original file to another prefix    

        elif str(file).__contains__('.zip'):
            OLD_PATH = file.key
            NEW_PREFIX = 'old-data/'+OLD_PATH

            try:
                s3.Object(bucket, NEW_PREFIX).copy_from(CopySource=f'{bucket}/{OLD_PATH}')
                s3.Object(bucket, OLD_PATH).delete()
            except:
                pass