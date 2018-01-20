FROM python:3.5 AS builder

ENV PYTHONUNBUFFERED 1

RUN mkdir -p /opt/mongodb_operator
WORKDIR /opt/mongodb_operator

COPY Pipfile \
     Pipfile.lock \
     mongodb_operator.py \
     ca-config.json \
     /opt/mongodb_operator/

COPY mongodb_operator/ \
    /opt/mongodb_operator/mongodb_operator/

ADD https://pkg.cfssl.org/R1.2/cfssl_linux-amd64 /opt/mongodb_operator/cfssl
RUN chmod +x /opt/mongodb_operator/cfssl

RUN pip install pipenv \
    && pipenv install \
    && ln -s `pipenv --py` /root/.local/share/virtualenvs/python

FROM python:3.5-slim

COPY --from=builder /root/.local/share/virtualenvs /root/.local/share/virtualenvs
COPY --from=builder /opt/mongodb_operator /opt/mongodb_operator

WORKDIR /opt/mongodb_operator
ENV PATH /root/.local/share/virtualenvs:$PATH

ENTRYPOINT ["python", "mongodb_operator.py"]
