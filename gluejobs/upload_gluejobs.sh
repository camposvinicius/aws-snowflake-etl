#!/bin/bash

cd gluejobs

aws s3 cp glue_job_1.py s3://data-codeszone
aws s3 cp glue_job_2.py s3://data-codeszone