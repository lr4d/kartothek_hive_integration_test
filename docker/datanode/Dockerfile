FROM hadoop-base:latest

HEALTHCHECK CMD curl -f http://localhost:50075/ || exit 1

#Create directory for datanode
ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data

COPY datanode/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 50075

CMD ["/run.sh"]
