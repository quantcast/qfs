FROM centos
RUN  yum install -y \
     make cmake \
     boost-devel gcc-c++ openssl-devel \
     libuuid-devel git \
     java-1.7.0-openjdk-devel maven
RUN  mkdir -p /code/qfs
ADD  . /code/qfs/.
WORKDIR /code/qfs

CMD ["make"]

