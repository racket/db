#!/bin/bash

set -e -u
set -o pipefail

# In normal mode: starts or stops a database server container.

# In wait mode (-w): starts a database server and waits for either
# - keyboard interrupt (Ctrl-C) => shuts down the server
# - server container shutdown => prints message

# One possible pattern of usage:
#   $ sudo bin/docker-util.sh pg        # start server
#   $ racket all-tests.rkt -t pg        # run tests
#   $ sudo bin/docker-util.sh stop      # stop server

# Another possible pattern of usage:
#   $ sudo bin/docker-util.sh -w pg &   # start server
#   $ racket all-tests.rkt -t pg        # run tests
#   $ fg                                # <Ctrl-C> to stop server

# The configurations below are compatible with "../test-dsn.rktd".

# See also https://github.com/racket/db/wiki/testing-with-docker.

# ------------------------------------------------------------

# All of the commands below create a container named 'testdb', run it in the
# background (-d), and set the container for automatic deletion when it stops
# (--rm).

COMMON_OPTS="--name testdb --rm -d"

start_pg_md5() {
    need_image "postgres"
    docker run $COMMON_OPTS --publish 5432:5432 \
           -e POSTGRES_USER=rkt \
           -e POSTGRES_PASSWORD=rktpwd \
           -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
           -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
           postgres \
           -c 'ssl=on' \
           -c 'ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem' \
           -c 'ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key'
}

start_pg_scram() {
    need_image "postgres"
    docker run $COMMON_OPTS --publish 5432:5432 \
           -e POSTGRES_USER=rkt \
           -e POSTGRES_PASSWORD=rktpwd \
           -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
           -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
           postgres \
           -c 'ssl=on' \
           -c 'ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem' \
           -c 'ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key'
}

start_mysql() {
    need_image "mysql"
    docker run $COMMON_OPTS --publish 3306:3306 \
           -e MYSQL_ROOT_PASSWORD=myrootpwd \
           -e MYSQL_USER=rkt \
           -e MYSQL_PASSWORD=rktpwd \
           -e MYSQL_DATABASE=rkt \
           mysql
}

start_mariadb() {
    need_image "mariadb"
    docker run $COMMON_OPTS --publish 3306:3306 \
           -e MYSQL_ROOT_PASSWORD=myrootpwd \
           -e MYSQL_USER=rkt \
           -e MYSQL_PASSWORD=rktpwd \
           -e MYSQL_DATABASE=rkt \
           mariadb
}

start_oracle() {
    if [ -z `docker image ls -q oracle/database:11.2.0.2-xe` ] ; then
        echo "Missing image 'oracle/database:11.2.0.2-xe'..." > /dev/stderr
        echo "Build from https://github.com/oracle/docker-images" > /dev/stderr
        exit 1
    fi
    docker run $COMMON_OPTS --publish 1521:1521 --publish 5500:5500 \
           -e ORACLE_PWD=orapwd \
           --shm-size=1g \
           oracle/database:11.2.0.2-xe
}

start_db2() {
    need_image ibmcom/db2
    docker run $COMMON_OPTS -p 50000:50000 \
           --privileged=true -e LICENSE=accept \
           -e DB2INST1_PASSWORD=db2pwd \
           -e DBNAME=testdb \
           ibmcom/db2
}

start_mssql() {
    need_image "mcr.microsoft.com/mssql/server"
    docker run $COMMON_OPTS --publish 1433:1433 \
           -e 'ACCEPT_EULA=Y' \
           -e 'SA_PASSWORD=abcdEFGH89!' \
           -e 'MSSQL_PID=Express' \
           mcr.microsoft.com/mssql/server
}

start_cassandra() {
    need_image "cassandra"
    docker run $COMMON_OPTS --publish 9042:9042 \
           cassandra
}

# ------------------------------------------------------------

need_image() {
    images=`docker image ls -q "$1"`
    if [ -z "$images" ] ; then
        echo "Missing image '$1'" > /dev/stderr
        echo "Fetch with 'docker pull $1'" > /dev/stderr
        exit 1
    fi
}

usage_exit() {
    echo "Usage: docker-util.sh [-w] {stop | <dbname>}" > /dev/stderr
    echo "with <dbname> in: $DBNAMES" > /dev/stderr
    exit 1
}

cleanup() {
    echo "Stopping 'testdb' container."
    docker stop testdb
    # Stopping the container should automatically delete it (because it was
    # started with the --rm option), but in case it doesn't, it can be deleted
    # with the following command: 'docker rm --force testdb'.
}

# ------------------------------------------------------------

DBNAMES="pg pg-scram my mariadb oracle db2 mssql cassandra"

WAITMODE=no     # "no" or "yes"

while [ "$#" -gt 0 ] ; do
    case "$1" in
        -w)
            WAITMODE=yes
            shift 1
            ;;
        *)
            break
            ;;
    esac
done

if [ "$#" -ne "1" ] ; then
    usage_exit
fi

trap 'echo ; cleanup' SIGINT
case "$1" in
    stop)
        cleanup
        exit 0
        ;;
    pg)
        start_pg_md5
        ;;
    pg-scram)
        start_pg_scram
        ;;
    my)
        start_mysql
        ;;
    mariadb)
        start_mariadb
        ;;
    oracle)
        start_oracle
        ;;
    db2)
        start_db2
        ;;
    mssql)
        start_mssql
        ;;
    cassandra)
        start_cassandra
        ;;
    *)
        usage_exit
        ;;
esac

echo "Container 'testdb' started for '$1' database server."
echo "The server might not be available immediately."

case "$WAITMODE" in
    no)
        echo "Run 'docker-util.sh stop' to stop the server."
        exit 0
        ;;
    yes)
        echo "Press Ctrl-C to shut down..."
        docker wait testdb ; echo "Container was shut down from somewhere else."
        ;;
esac
