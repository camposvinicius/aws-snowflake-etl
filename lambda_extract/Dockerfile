FROM public.ecr.aws/lambda/python:3.9

ENV AWS_ACCESS_KEY_ID=<YOUR_AWS_ACCESS_KEY_ID>
ENV AWS_SECRET_ACCESS_KEY=<YOUR_AWS_SECRET_ACCESS_KEY>
ENV AWS_DEFAULT_REGION=<YOUR_AWS_DEFAULT_REGION>

COPY requirements.txt ./  
  
RUN python3 -m pip install --upgrade pip  
RUN python3 -m pip install -r requirements.txt  
  
COPY . .
CMD [ "lambda_function.lambda_handler" ]