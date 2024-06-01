#!/bin/bash

print_usage() {
    cat <<EOF
Usage: docker-util.sh [-w] {stop | <dbname>} [<dockerimage>]
with <dbname> in: pg pg-scram my mariadb oracle db2 mssql cassandra
and optional <dockerimage> as in postgres, postgres:12, mariadb:latest, etc.
May need to be run with 'sudo' depending on docker configuration.
Run 'docket-util.sh -h' for extended help.
EOF
}

print_help() {
    cat <<EOF
CONFIGURATIONS

The configurations below are compatible with "../test-dsn.rktd".
See also https://github.com/racket/db/wiki/testing-with-docker.

NORMAL MODE

In normal mode, this script starts or stops a database server container.

Example usage:
  $ sudo bin/docker-util.sh pg              # start server
  $ racket all-tests.rkt -t pg              # run tests
  $ sudo bin/docker-util.sh stop            # stop server

Testing with a specific docker image:
  $ sudo bin/docker-util.sh pg postgres:12  # start server: PostgreSQL 12.x
  $ racket all-tests.rkt -t pg              # run tests
  $ sudo bin/docker-util.sh stop            # stop server

WAIT MODE

In wait mode (-w), this script starts a database server and waits for either
- keyboard interrupt (Ctrl-C) => shuts down the server
- server container shutdown => prints message

Example usage:
  $ sudo bin/docker-util.sh -w pg &         # start server, background
  $ racket all-tests.rkt -t pg              # run tests
  $ fg                                      # then <Ctrl-C> to stop server

SUDO

Note: 'sudo' is necessary for default docker configuration, but may be
unnecessary for rootless docker, podman, etc.

ENVIRONMENT VARIABLES

- DOCKER: Set this to the full path of a docker-compatible executable,
  otherwise the script defaults to "docker" or "podman", if available.
EOF
}

# ------------------------------------------------------------

DOCKER="${DOCKER:-}"
if [ -z "$DOCKER" ] ; then
    DOCKER=`which docker`
fi
if [ -z "$DOCKER" ] ; then
    DOCKER=`which podman`
fi

set -e -u
set -o pipefail

check_docker_available() {
    if [ -z "$DOCKER" ] ; then
        echo "docker-util: cannot find docker-compatible command" >&2
        echo "Run 'docket-util.sh -h' for extended help." >&2
        exit 1
    elif [ ! -f "$DOCKER" ] ; then
        echo "docker-util: docker command ($DOCKER) not found" >&2
        echo "Run 'docket-util.sh -h' for extended help." >&2
        exit 1
    elif [ ! -x "$DOCKER" ] ; then
        echo "docker-util: docker command ($DOCKER) is not executable" >&2
        echo "Run 'docket-util.sh -h' for extended help." >&2
        exit 1
    fi
}

# ------------------------------------------------------------

# All of the commands below
# - create a container named 'testdb',
# - run it in the background (-d), and
# - set the container for automatic deletion when it stops (--rm)
COMMON_OPTS="--name testdb --rm -d"
DEBUG_OPTS="--name testdb --rm"
DBIMAGE=""  # mutated below

start_pg_md5() {
    DBIMAGE="${DBIMAGE:-postgres}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 5432:5432 \
           -e POSTGRES_USER=rkt \
           -e POSTGRES_PASSWORD=rktpwd \
           -e POSTGRES_HOST_AUTH_METHOD=md5 \
           "$DBIMAGE" \
           -c 'ssl=on' \
           -c 'ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem' \
           -c 'ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key'
}

start_pg_scram() {
    DBIMAGE="${DBIMAGE:-postgres}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 5432:5432 \
           -e POSTGRES_USER=rkt \
           -e POSTGRES_PASSWORD=rktpwd \
           -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
           -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
           "$DBIMAGE" \
           -c 'ssl=on' \
           -c 'ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem' \
           -c 'ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key'
}

start_mysql() {
    DBIMAGE="${DBIMAGE:-mysql}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 3306:3306 \
           -e MYSQL_ROOT_PASSWORD=myrootpwd \
           -e MYSQL_USER=rkt \
           -e MYSQL_PASSWORD=rktpwd \
           -e MYSQL_DATABASE=rkt \
           "$DBIMAGE"
}

start_oracle() {
    DBIMAGE="${DBIMAGE:-container-registry.oracle.com/database/free}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 1521:1521 --publish 5500:5500 \
           -e ORACLE_SID=FREE \
           -e ORACLE_PWD=orapwd \
           --shm-size=1g \
           "$DBIMAGE"
}

start_db2() {
    DBIMAGE="${DBIMAGE:-ibmcom/db2}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS -p 50000:50000 \
           --privileged=true -e LICENSE=accept \
           -e DB2INST1_PASSWORD=db2pwd \
           -e DBNAME=testdb \
           "$DBIMAGE"
}

start_mssql() {
    DBIMAGE="${DBIMAGE:-mcr.microsoft.com/mssql/server}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 1433:1433 \
           -e 'ACCEPT_EULA=Y' \
           -e 'SA_PASSWORD=abcdEFGH89!' \
           -e 'MSSQL_PID=Express' \
           "$DBIMAGE"
}

start_cassandra() {
    DBIMAGE="${DBIMAGE:-cassandra}"
    need_image "$DBIMAGE"
    "$DOCKER" run $COMMON_OPTS --publish 9042:9042 \
           "$DBIMAGE"
}

# ------------------------------------------------------------

need_image() {
    images=`"$DOCKER" image ls -q "$1"`
    if [ -z "$images" ] ; then
        echo "Missing image '$1'" >&2
        if [ -z "${2-}" ] ; then
            echo "Fetch with '$DOCKER pull $1'" >&2
        else
            echo "$2" >&2
        fi
        exit 1
    fi
}

usage_exit() {
    print_usage >&2
    exit 1
}

help_exit() {
    print_usage >&2
    echo
    print_help >&2
    exit 0
}

cleanup() {
    echo "Stopping 'testdb' container."
    "$DOCKER" stop testdb
    # Stopping the container should automatically delete it (because it was
    # started with the --rm option), but in case it doesn't, it can be deleted
    # with the following command: 'docker rm --force testdb'.
}

# ------------------------------------------------------------

WAITMODE=no     # "no" or "yes"

while [ "$#" -gt 0 ] ; do
    case "$1" in
        -w)
            WAITMODE=yes
            shift 1
            ;;
        -d)
            COMMON_OPTS="$DEBUG_OPTS"
            shift 1
            ;;
        -h)
            help_exit
            ;;
        *)
            break
            ;;
    esac
done

check_docker_available

case "$#" in
    1)
        DB="$1"
        DBIMAGE=""
        ;;
    2)
        DB="$1"
        DBIMAGE="$2"
        ;;
    *)
        usage_exit
        ;;
esac

trap 'echo ; cleanup' SIGINT
case "$DB" in
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
        DBIMAGE="${DBIMAGE:-mariadb}"
        start_mysql
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

echo "Container 'testdb' started for '$DB' database server"
echo "using image '$DBIMAGE'."
echo "The server might not be available immediately."

case "$WAITMODE" in
    no)
        echo "Run 'docker-util.sh stop' to stop the server."
        exit 0
        ;;
    yes)
        echo "Press Ctrl-C to shut down..."
        "$DOCKER" wait testdb ; echo "Container was shut down from somewhere else."
        ;;
esac
