FROM continuumio/miniconda3

# netcat is necessary for the `wait_for_it` test in `entrypoint.sh`
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends netcat && \
    rm -rf /var/lib/apt/lists/*

# `PYTHON_VERSION` can be provided as an enviroment variable to execute the
# test with different versions (input must conform to conda standards)
ARG PYTHON_VERSION=3.7

RUN conda install -y python=${PYTHON_VERSION}
RUN pip install git+https://github.com/JDASoftwareGroup/kartothek@master pytest
# `pyhive` is installed after the pip install to prevent pip from changing pyhive
# dependencies
RUN conda install -y pyhive
# Show version information of packages
RUN conda list

ENV RUN_ON_TRAVIS="True"

COPY test_executor/test_hive_compatibility.py test_hive_compatibility.py

COPY common.sh /usr/local/bin/

COPY test_executor/entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

COPY test_executor/ci_or_local.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/ci_or_local.sh

ENTRYPOINT ["entrypoint.sh"]
CMD ci_or_local.sh
