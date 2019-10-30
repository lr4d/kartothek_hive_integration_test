FROM hadoop-base:latest

HEALTHCHECK CMD curl -f http://localhost:50070/ || exit 1

#Create directory for namenode
ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name

COPY namenode/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 50070

CMD ["/run.sh"]
