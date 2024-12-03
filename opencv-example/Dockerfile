FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive


RUN apt-get clean && \
    apt-get update && \ 
    apt-get install -y --no-install-recommends --fix-missing \
    python3 \
    python3-pip \
    vim \ 
    wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install opencv-python-headless matplotlib

WORKDIR /app/

RUN mkdir output

COPY ./detect.py ./
COPY ./stop-sign.jpg ./
COPY ./stop_data.xml ./

CMD ["python3", "detect.py"]

