FROM python:3.6.6-alpine3.6

RUN echo "http://mirror.leaseweb.com/alpine/edge/testing" >> /etc/apk/repositories && \
        apk update && \ 
	apk add --no-cache postgresql-dev gcc python3-dev musl-dev libxslt-dev geos-dev zip && \
	apk add gcc musl-dev python3-dev libffi-dev libxslt-dev && \
	pip install --upgrade pip

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

COPY . /WM
WORKDIR /WM

