#!/usr/bin/env bash

# Initialize Terraform
#terraform init

# Copy Python files
cp ./football_pipeline/match_results/football_data_co_uk.py ./lambda_payloads/

# Too difficult to resolve dependencies via the method below:

# Use Python to get the site-packages directory path
#SITE_PACKAGES=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")
# Copy necessary packages from the site-packages directory
#cp -r ${SITE_PACKAGES}/requests ./football_pipeline/lambda_payloads/
#cp -r ${SITE_PACKAGES}/boto3 ./football_pipeline/lambda_payloads/
#cp -r ${SITE_PACKAGES}/bs4 ./football_pipeline/lambda_payloads/

pip install boto3 -t ./lambda_payloads/
pip install beautifulsoup4 -t ./lambda_payloads/
pip install requests -t ./lambda_payloads/
pip install pandas -t ./lambda_payloads/

# Zip the payload
cd ./lambda_payloads/
zip -r ../payload.zip *

# Execute Terraform plan
cd ../terraform/
terraform plan
