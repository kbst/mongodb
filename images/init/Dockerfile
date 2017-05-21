FROM debian:jessie

RUN set -x \
	&& apt-get update \
	&& apt-get install -y ansible

ADD https://storage.googleapis.com/kubernetes-release/release/v1.5.1/bin/linux/amd64/kubectl /usr/local/bin/kubectl
RUN chmod +x /usr/local/bin/kubectl

ADD https://pkg.cfssl.org/R1.2/cfssl_linux-amd64 /usr/local/bin/cfssl
RUN chmod +x /usr/local/bin/cfssl

ADD https://pkg.cfssl.org/R1.2/cfssljson_linux-amd64 /usr/local/bin/cfssljson
RUN chmod +x /usr/local/bin/cfssljson

ADD src /opt/mongodb-init

WORKDIR /opt/mongodb-init
CMD bash
