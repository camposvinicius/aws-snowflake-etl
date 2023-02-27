#!/bin/bash

cd lambda_extract

aws ecr get-login-password --region <YOUR_REGION_NAME> | docker login --username AWS --password-stdin <YOUR_ACCOUNT_ID>.dkr.ecr.<YOUR_REGION_NAME>.amazonaws.com
docker build -t lambda-extract-image .
docker tag lambda-extract-image:latest <YOUR_ACCOUNT_ID>.dkr.ecr.<YOUR_REGION_NAME>.amazonaws.com/lambda-extract-image:latest
docker push <YOUR_ACCOUNT_ID>.dkr.ecr.<YOUR_REGION_NAME>.amazonaws.com/lambda-extract-image:latest