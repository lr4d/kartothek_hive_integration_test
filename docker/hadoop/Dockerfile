FROM debian:9

#Install all packages and java for debian:9
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-8-jdk \
      net-tools \
      curl \
      netcat \
      gnupg \
    && rm -rf /var/lib/apt/lists/*
      
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

#Get the GPG keys for the Hadoop
RUN curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS

RUN gpg --import KEYS

# Set HADOOP_VERSION from arg if provided at build, env if provided at run, or default
ARG HADOOP_VERSION
ENV HADOOP_VERSION=${HADOOP_VERSION:-2.7.4}
ENV HADOOP_URL https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

#Install Hadoop tar ball
RUN set -x \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc \
    && gpg --verify /tmp/hadoop.tar.gz.asc \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz*

#create a Soft-link for the hadoop home location
RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop

RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs

RUN mkdir /hadoop-data

ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV USER=root
ENV PATH $HADOOP_PREFIX/bin/:$PATH

#mapred-site.xml should be copied to /etc/hadoop/ folder
RUN cp /etc/hadoop/mapred-site.xml.template /etc/hadoop/mapred-site.xml

COPY common.sh /common.sh

COPY hadoop/entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
