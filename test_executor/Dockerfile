FROM continuumio/miniconda3

# netcat is necessary for the `wait_for_it` test in `entrypoint.sh`
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends netcat && \
    rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.7

RUN conda install -y python=${PYTHON_VERSION}

RUN pip install git+https://github.com/JDASoftwareGroup/kartothek@master

RUN conda install -y pyhive

COPY startup.py /usr/local/bin/
RUN chmod +x /usr/local/bin/startup.py

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["entrypoint.sh"]
CMD startup.py
