FROM python:3.5
RUN echo 'debconf debconf/frontend select Noninteractive' \
    | debconf-set-selections \
    && apt-get update -qq \
    && apt-get upgrade -y

ADD https://storage.googleapis.com/kubernetes-release/release/v1.6.2/bin/linux/amd64/kubectl /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

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
    && pipenv install

ENTRYPOINT ["pipenv", "run", "python", "mongodb_operator.py"]
