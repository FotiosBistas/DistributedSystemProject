# To enable ssh & remote debugging on app service change the base image to the one below 
#FROM mcr.microsoft.com/azure-functions/python:4-python3.11-appservice
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Install dependencies
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt-get update && apt-get install -y 
#    curl gnupg && \
#    curl -sL https://deb.nodesource.com/setup_16.x | bash - && \
#    apt-get install -y nodejs && \
#    npm install -g azure-functions-core-tools@4 --unsafe-perm true && \
#    apt-get clean
#

COPY requirements.txt ./

RUN pip install -r ./requirements.txt

COPY . /home/site/wwwroot/

