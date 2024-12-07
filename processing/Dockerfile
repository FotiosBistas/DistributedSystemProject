FROM mcr.microsoft.com/azure-functions/python:4-python3.12

# Install dependencies
WORKDIR /home/site/wwwroot
COPY . ./

RUN apt-get update && apt-get install -y \
    curl gnupg && \
    curl -sL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y nodejs && \
    npm install -g azure-functions-core-tools@4 --unsafe-perm true && \
    apt-get clean

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# Expose Azure Functions default port
EXPOSE 7071

CMD ["func", "start"]


