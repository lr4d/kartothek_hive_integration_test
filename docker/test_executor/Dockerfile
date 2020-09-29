FROM continuumio/miniconda3

ENV BASH_ENV ~/.bashrc
SHELL ["/bin/bash", "-c"]

# netcat is necessary for the `wait_for_it` test in `entrypoint.sh`
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends netcat && \
    rm -rf /var/lib/apt/lists/*

COPY test_executor/conda-environment.yml environment.yml

ARG conda_env=test
RUN conda env create -n $conda_env -f environment.yml
# Set this as the default conda env
ENV CONDA_DEFAULT_ENV ${conda_env}

# Show version information of packages
RUN conda list

COPY test_executor/test_hive_compatibility.py .

COPY common.sh /usr/local/bin/

COPY test_executor/entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

COPY test_executor/ci_or_local.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/ci_or_local.sh

ENTRYPOINT ["entrypoint.sh"]
CMD ci_or_local.sh
