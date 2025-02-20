FROM rknop/devuan-daedalus-rknop AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y \
         python3 tmux postgresql-client \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
ENV LESS=-XLRi

# This will bind-mount the same flask directory as the webserver
RUN mkdir /flask
WORKDIR /flask

# ======================================================================
FROM base AS build

RUN DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get update \
    && DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get -y install -y python3-pip python3-venv git libpq-dev

RUN mkdir /venv
RUN python3 -mvenv /venv

RUN source /venv/bin/activate && \
    pip --no-cache install \
       numpy \
       pandas \
       psycopg2

# ======================================================================
FROM base AS queryrunner

COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

ENTRYPOINT [ "python", "/flask/long_query_runner.py", "--loop", "--sleep-time", "10", "--num-runners", "10" ]
