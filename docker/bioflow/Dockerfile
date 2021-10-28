FROM anna
MAINTAINER "xtaotech" <jasonzhang@xtaotech.com>

RUN rm -rf /etc/yum.repos.d/*
ADD yum.conf /etc/yum.conf
ADD xtao.repo /etc/yum.repos.d/xtao.repo
RUN ldconfig -f /etc/ld.so.conf

#RUN yum clean all && yum makecache


RUN mkdir -p /opt/bioflow

COPY bioflow /opt/bioflow/bioflow
COPY biocli /opt/bioflow/biocli
COPY bioadm /opt/bioflow/bioadm

COPY marathon.sh /marathon.sh

WORKDIR /opt/bioflow
CMD [ "/marathon.sh" ]
