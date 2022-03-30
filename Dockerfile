FROM python:3.9-bullseye
RUN apt update
RUN apt install elpa-magit -y
RUN apt install git-all python3-dev python3-pip python3-setuptools cmake build-essential libssl-dev -y
WORKDIR /usr/src/app
ADD . /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8080
CMD ./micro-svc-demo.py
