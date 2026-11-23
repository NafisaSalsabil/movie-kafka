
FROM amazoncorretto:17

RUN yum install -y python3 python3-pip python3-setuptools && \
    yum clean all

WORKDIR /app

RUN pip3 install --no-cache-dir \
    pyspark==3.5.8 \
    pyarrow \
    kafka-python

ENTRYPOINT []

CMD ["python3", "starter.py"]

