#lang scribble/doc
@(require scribble/manual
          scribble/eval
          scribble/struct
          racket/sandbox
          "config.rkt"
          (for-label db
                     setup/dirs))

@title[#:tag "notes"]{Notes}

This section discusses issues related to specific database systems.


@section[#:tag "connecting-to-server"]{Local Sockets for PostgreSQL and MySQL Servers}

PostgreSQL and MySQL servers are sometimes configured by default to
listen only on local sockets (also called ``unix domain
sockets''). This library provides support for communication over local
sockets on Linux and Mac OS. If local socket communication is not
available, the server must be reconfigured to listen on a TCP port.

The socket file for a PostgreSQL server is located in the directory
specified by the @tt{unix_socket_directory} variable in the
@tt{postgresql.conf} server configuration file.  For example, on
Ubuntu 11.04 running PostgreSQL 8.4, the socket directory is
@tt{/var/run/postgresql} and the socket file is
@tt{/var/run/postgresql/.s.PGSQL.5432}. Common socket paths may be
searched automatically using the @racket[postgresql-guess-socket-path]
function.

The socket file for a MySQL server is located at the path specified by
the @tt{socket} variable in the @tt{my.cnf} configuration file. For
example, on Ubuntu 11.04 running MySQL 5.1, the socket is located at
@tt{/var/run/mysqld/mysqld.sock}. Common socket paths for MySQL can be
searched using the @racket[mysql-guess-socket-path] function.


@section{PostgreSQL Database Character Encoding}

In most cases, a database's character encoding is irrelevant, since
the connect function always requests translation to Unicode (UTF-8)
when creating a connection. If a PostgreSQL database's character
encoding is @tt{SQL_ASCII}, however, PostgreSQL will not honor the
connection encoding; it will instead send untranslated octets, which
will cause corrupt data or internal errors in the client connection.

To convert a PostgreSQL database from @tt{SQL_ASCII} to something
sensible, @tt{pg_dump} the database, recode the dump file (using a
utility such as @tt{iconv}), create a new database with the desired
encoding, and @tt{pg_restore} from the recoded dump file.


@section{PostgreSQL Authentication}

PostgreSQL supports a large variety of
@hyperlink["http://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html"]{authentication
mechanisms}, controlled by the @tt{pg_hba.conf} server configuration
file. This library currently works with the following authentication methods:
@itemlist[
@item{@tt{plain} (and @tt{ldap}, @tt{pam}, @tt{radius}): cleartext password, only if
explicitly allowed (see @racket[postgresql-connect])}
@item{@tt{md5}: MD5-hashed password}
@item{@tt{scram-sha-256}: password-based challenge/response protocol}
@item{@tt{peer}: only for local sockets}
]
The @tt{gss}, @tt{sspi}, @tt{krb5}, @tt{pam}, and @tt{ldap} methods
are not supported.

@history[#:changed "1.2" @elem{Added @tt{scram-sha-256} support.}]

@section[#:tag "postgresql-timestamp-tz"]{PostgreSQL Timestamps and Time Zones}

PostgreSQL's @tt{timestamp with time zone} type is inconsistent with
the SQL standard (probably), inconsistent with @tt{time with time
zone}, and potentially confusing to PostgreSQL newcomers.

A @tt{time with time zone} is essentially a @tt{time} structure with
an additional field storing a time zone offset. In contrast, a
@tt{timestamp with time zone} has no fields beyond those of
@tt{timestamp}. Rather, it indicates that its datetime fields should
be interpreted as a UTC time. Thus it represents an absolute point in
time, unlike @tt{timestamp without time zone}, which represents local
date and time in some unknown time zone (possibly---hopefully---known
the the database designer, but unknown to PostgreSQL).

When a @tt{timestamp with time zone} is created from a source without
time zone information, the session's @tt{TIME ZONE} setting is used to
adjust the source to UTC time. When the source contains time zone
information, it is used to adjust the timestamp to UTC time. In either
case, the time zone information is @emph{discarded} and only the UTC
timestamp is stored. When a @tt{timestamp with time zone} is rendered
as text, it is first adjusted to the time zone specified by the
@tt{TIME ZONE} setting (or by
@hyperlink["http://www.postgresql.org/docs/8.0/static/functions-datetime.html#FUNCTIONS-DATETIME-ZONECONVERT"]{@tt{AT
TIME ZONE}}) and that offset is included in the rendered text.

This library receives timestamps in binary format, so the time zone
adjustment is not applied, nor is the session's @tt{TIME ZONE} offset
included; thus all @racket[sql-timestamp] values in a query result
have a @racket[tz] field of @racket[0] (for @tt{timestamp with time
zone}) or @racket[#f] (for @tt{timestamp without time
zone}). (Previous versions of this library sent and received
timestamps as text, so they received timestamps with adjusted time
zones.)


@section{MySQL Authentication}

As of version 5.5.7, MySQL supports
@hyperlink["http://dev.mysql.com/doc/mysql-security-excerpt/5.5/en/pluggable-authentication.html"]{authentication
plugins}. The only plugins currently supported by this library are
@tt{mysql_native_password} (the default) and @tt{mysql_old_password},
which corresponds to the password authentication mechanisms used since
version 4.1 and before 4.1, respectively.


@section{MySQL @tt{CALL}ing Stored Procedures}

MySQL @tt{CALL} statements can be executed only if they return at most
one result set and contain no @tt{OUT} or @tt{INOUT} parameters.


@section{Cassandra Authentication}

Cassandra, like MySQL, supports
@hyperlink["http://cassandra.apache.org/doc/latest/operating/security.html#authentication"]{authentication
plugins}. The only plugins currently supported by this library are
@tt{AllowAllAuthenticator} and @tt{PasswordAuthenticator}.


@section[#:tag "sqlite3-requirements"]{SQLite Requirements}

SQLite support requires the appropriate native library.

@itemlist[

@item{On Windows, the library is @tt{sqlite3.dll}. It is included in
the Racket distribution.}

@item{On Mac OS, the library is @tt{libsqlite3.0.dylib}, which is
included (in @tt{/usr/lib}) in Mac OS version 10.4 onwards.}

@item{On Linux, the library is @tt{libsqlite3.so.0}. It is included in
the @tt{libsqlite3-0} package in Debian/Ubuntu and in the @tt{sqlite}
package in Red Hat.}
]


@section[#:tag "ffi-concurrency"]{FFI-Based Connections and Concurrency}

@tech{Wire-based connections} communicate using
@tech/reference{ports}, which do not cause other Racket threads to
block. In contrast, an FFI call causes all Racket threads to block
until it completes, so @tech{FFI-based connections} can degrade the
interactivity of a Racket program, particularly if long-running
queries are performed using the connection. This problem can be
avoided by creating the FFI-based connection in a separate
@tech/reference{place} using the @racket[#:use-place] keyword
argument. Such a connection will not block all Racket threads during
queries; the disadvantage is the cost of creating and communicating
with a separate @tech/reference{place}.


@section[#:tag "odbc-requirements"]{ODBC Requirements}

ODBC requires the appropriate driver manager native library as well as
driver native libraries for each database system you want use ODBC to
connect to.

@itemlist[

@item{On Windows, the driver manager is @tt{odbc32.dll}, which is
included automatically with Windows.}

@item{On Mac OS, the driver manager is @tt{libiodbc.2.dylib}
(@hyperlink["http://www.iodbc.org"]{iODBC}), which is included (in
@tt{/usr/lib}) in Mac OS version 10.2 onwards.}

@item{On Linux, the driver manager is @tt{libodbc.so.{2,1}}
(@hyperlink["http://www.unixodbc.org"]{unixODBC}---iODBC is not
supported). It is available from the @tt{unixodbc} package in
Debian/Ubuntu and in the @tt{unixODBC} package in Red Hat.}
]

In addition, you must install the appropriate ODBC Drivers and
configure Data Sources. Refer to the ODBC documentation for the
specific database system for more information.


@section[#:tag "odbc-status"]{ODBC Status}

ODBC support is experimental. The behavior of ODBC connections can
vary widely depending on the driver in use and even the configuration
of a particular data source.

The following sections describe the configurations that this library
has been tested with. Reports of success or failure on other platforms
or with other drivers would be appreciated.


@subsection{DB2 ODBC Driver}

IBM DB2 ODBC drivers were tested with the following software configuration:

@itemlist[
@item{Platform: Centos 7.4 on x86_64}
@item{Database: DB2 Express-C for Linux x64 v11.1}
@item{Driver: ODBC for DB2 (included with DB2 Express-C)}
]

This driver seems to require environment variables to be set using the
provided scripts (eg, @tt{source /home/db2inst1/sqllib/db2profile}).

Known issues:
@itemlist[

@item{The driver does not support the standard @tt{SQL_C_NUMERIC}
structure for retrieving @tt{DECIMAL}/@tt{NUMERIC} fields.

@bold{Fix: } Use @racket[#:quirks '(no-c-numeric)] with @racket[odbc-connect].}

]

@subsection{Oracle ODBC Driver}

Oracle ODBC drivers were tested with the following software configuration:

@itemlist[
@item{Platform: Centos 7.4 on x86_64}
@item{Database: Oracle XE 11g (11.2.0)}
@item{Drivers: Oracle Instant Client ODBC (11.2.0 and 12.2.0)}
]

Typical installations of the drivers require the @tt{LD_LIBRARY_PATH}
environment variable to be set to the driver's installed @tt{lib}
directory (ie, the directory containing @tt{libsqora.so}) so the
driver can find its sibling shared libraries.

Known issues:
@itemlist[

@item{With the @racket[#:strict-parameter-types? #t] option,
parameters seem to be always assigned the type @tt{varchar}.

@bold{Fix: } Leave strict parameter types off (the default).}

@item{The driver does not support the @tt{SQL_C_BIGINT} format for
parameters or result fields. Consequently, passing large integers as
query parameters may fail.

@bold{Fix: } Use @racket[#:quirks '(no-c-bigint)] with
@racket[odbc-connect].}

@item{A field of type @tt{TIME} causes the driver to return garbage
for the typeid and type parameters. This usually causes an error with
a message like ``unsupported type; typeid: -29936'', but with a random
typeid value. (Oracle appears not to have a @tt{TIME} type, so this
bug might only appear when a value is explicitly @tt{CAST} as
@tt{TIME}---for some reason, that doesn't produce an error.)}

@item{Attempting to quit Racket with a connection still open may cause
Racket to hang. Specifically, the problem seems to be in the driver's
@tt{_fini} function.

@bold{Fix: } Close connections before exiting, either explicitly using
@racket[disconnect] or by shutting down their custodians.}

]

@subsection{SQL Server ODBC Driver}

Microsoft SQL Server ODBC drivers were tested with the following
software configuration:

@itemlist[
@item{Platform: Windows 10 on x86_64}
@item{Database: SQL Server Express 2017}
@item{Drivers: ODBC Driver 13 for SQL Server, SQL Server Native Client 11.0}
]

Known issues:
@itemlist[

@item{If queries are nested or interleaved---that is, a second query
is executed before the first query's results are completely
consumed---the driver might signal an error ``Connection is busy with
results for another command (SQLSTATE: HY000)''.

@bold{Fix: } Set the @tt{MARS_Connection} data source option to @tt{Yes} (see
@hyperlink["https://stackoverflow.com/questions/9017264/why-only-some-users-get-the-error-connection-is-busy"]{this page}). The ODBC Manager GUI does not expose the option, but it can be added @hyperlink["https://serverfault.com/questions/302169/odbc-sql-server-how-do-i-turn-on-multiple-active-result-sets"]{by editing the registry}.}

]
