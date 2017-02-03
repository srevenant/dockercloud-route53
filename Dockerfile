FROM python:3

ENV BASE=/app/dca
ENV PYTHONUNBUFFERED=true

WORKDIR $BASE

COPY requirements.txt $BASE
RUN pip3 install -r requirements.txt

COPY dockercloud-route53.py $BASE

ENTRYPOINT ["/app/dca/dockercloud-route53.py"]
