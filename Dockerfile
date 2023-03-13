# VERSION 2.2.5
# AUTHOR: Jesse Wei
# DESCRIPTION: Airflow container with SQLG and MSSQL
# BUILD: docker build --rm -t saastoolset/sqlg-airflow .
# SOURCE: https://github.com/saastoolset/sqlg-airflow
# Clone: https://github.com/puckel/docker-airflow



FROM python:3.9.6-bullseye
#FROM python:3.9-slim-bullseye
LABEL maintainer="Jesse_"
LABEL maintainer="saastoolset"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=2.2.5
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Oracle ENV, for next release
ENV LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib/
ENV ORACLE_HOME=/usr/lib/oracle/11.2/client64
ENV PATH="$ORACLE_HOME/bin:${PATH}"


# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
        rustc \
        cargo \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
#        krb5-user \
#        ldap-utils \
#        libffi6 \
#        libsasl2-2 \
#        libsasl2-modules \
#        libssl1.1 \
#        lsb-release \
#        sasl2-bin \
#        sqlite3 \
        unixodbc \
		unixodbc-dev \
		g++ \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
#    && pip install cx_Oracle \	
	&& pip install pyodbc \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install redis \
#    && pip install MarkupSafe \	
    && pip install 'apache-airflow-providers-microsoft-mssql' \
    && pip install airflow-provider-kafka \
#    && pip install 'apache-airflow-providers-oracle' \	
#    && pip install 'cryptography>=3.2' \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
#	&& apt-get install libaio1 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# For etl cp file permission
RUN  mkdir ${AIRFLOW_USER_HOME}/etl_base
RUN  find ${AIRFLOW_USER_HOME}/etl_base -type d -exec chmod +w {} +
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY config/webserver_config.py ${AIRFLOW_USER_HOME}/webserver_config.py
COPY config/conn.json ${AIRFLOW_USER_HOME}/conn.json
COPY config/var.json ${AIRFLOW_USER_HOME}/var.json

# For MSSQL driver and tools
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc


# # Copy oracle driver
# COPY client64 /usr/lib/oracle/11.2/client64
# COPY script/.bash_profile ${AIRFLOW_USER_HOME}/.bash_profile
# COPY script/query.py ${AIRFLOW_USER_HOME}/query.py
# 

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
