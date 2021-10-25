FROM bitnami/postgresql-repmgr:11.13.0-debian-10-r40
USER root
RUN apt update && apt install make gcc git -y
RUN git clone https://github.com/omniti-labs/pg_amqp.git
RUN cd pg_amqp && make && make install
