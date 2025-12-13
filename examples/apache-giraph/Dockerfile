FROM maven:3.9.0-eclipse-temurin-11

ENV HADOOP_VERSION=2.10.1
ENV GIRAPH_VERSION=1.3.0
ENV GIRAPH_HADOOP_CLASSIFIER=hadoop2

WORKDIR /opt

RUN apt-get update && apt-get install -y wget ssh rsync && rm -rf /var/lib/apt/lists/*

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

COPY giraph-dist-${GIRAPH_VERSION}-${GIRAPH_HADOOP_CLASSIFIER}-bin.tar.gz /opt/
RUN tar -xzf giraph-dist-${GIRAPH_VERSION}-${GIRAPH_HADOOP_CLASSIFIER}-bin.tar.gz -C /opt/ \
    && rm giraph-dist-${GIRAPH_VERSION}-${GIRAPH_HADOOP_CLASSIFIER}-bin.tar.gz

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV GIRAPH_HOME=/opt/giraph-${GIRAPH_VERSION}-${GIRAPH_HADOOP_CLASSIFIER}
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV CLASSPATH=$CLASSPATH:$GIRAPH_HOME/lib/*

COPY ./hadoop/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY ./hadoop/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY ./hadoop/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml
COPY ./../apps /opt/apps
RUN mkdir -p /data/input /data/output

WORKDIR /opt/apps/max-value
RUN mvn clean package -DskipTests

WORKDIR /opt/apps/shortest-distance-to-capital-city
RUN mvn clean package -DskipTests

WORKDIR /opt/apps/movie-recommendation
RUN mvn clean package -DskipTests

WORKDIR /opt

CMD ["bash"]
