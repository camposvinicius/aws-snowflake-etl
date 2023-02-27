import boto3    

BUCKETS = [
    'data-rawzone',
    'data-queryresultszone',
    'data-processingzone',
    'data-deliveryzone',
    'data-codeszone'
]

def deleting_s3_resources():

    s3 = boto3.resource('s3')

    for bucket in BUCKETS:

        print(f'\nDeleting from bucket {bucket} ...')
        bucket = s3.Bucket(bucket)
        bucket.objects.all().delete()

def deleting_ecr_latest_image(repo_name):

    client = boto3.client('ecr')

    response = client.list_images(repositoryName=repo_name)
    latest_image = [image for image in response['imageIds'] if image['imageTag'] == 'latest']    

    response = client.batch_delete_image(repositoryName=repo_name, imageIds=latest_image)

    print('\nImage was deleted.')

if __name__ == "__main__":    

    deleting_ecr_latest_image('lambda-extract-image')
    deleting_s3_resources()