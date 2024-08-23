FROM ubuntu:22.04

RUN apt-get update -y \
&& apt-get install -y software-properties-common \
&& add-apt-repository ppa:openjdk/ppa \
&& apt-get install openjdk-17-jdk -y \
&& apt-get install python3-pip -y \
&& apt-get -y install git \
&& apt-get install -y python3.10 \
&& ln -sf python3 /usr/bin/python \
&& export JAVA_HOME \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME
RUN apt install -y pip
RUN pip install --upgrade pip setuptools pytest pytest-snapshot substrait pyarrow protobuf duckdb filelock datafusion==40.1.0 ibis_substrait JPype1 substrait-validator

WORKDIR /consumer-testing
COPY . .

CMD git submodule update --init \
&& ./build-and-copy-isthmus-shadow-jar.sh \
&& /usr/bin/python -mpytest -m produce_substrait_snapshot --producer=IsthmusProducer substrait_consumer/tests/functional/extension_functions
