#lang scribble/doc
@(require scribble/manual
          scribble/eval
          scribble/struct
          racket/sandbox
          racket/runtime-path
          "config.rkt"
          "tabbing.rkt"
          (for-label racket/base db db/util/geometry db/util/postgresql db/util/mysql
                     racket/flonum data/bit-vector json))

@(define-runtime-path log-file "log-for-sql-types.rktd")
@(define the-eval (make-pg-eval log-file #t))

@title[#:tag "sql-types"]{SQL Types and Conversions}

@declare-exporting[db db/base #:use-sources (db/base)]

Connections automatically convert query results to appropriate Racket
types. Likewise, query parameters are accepted as Racket values and
converted to the appropriate SQL type.

@examples[#:eval the-eval
(query-value pgc "select count(*) from the_numbers")
(query-value pgc "select false")
(query-value pgc "select 1 + $1" 2)
]

If a query result contains a column with a SQL type not supported by
this library, an exception is raised. As a workaround, cast the column
to a supported type:

@examples[#:eval the-eval
(query-value pgc "select inet '127.0.0.1'")
(query-value pgc "select cast(inet '127.0.0.1' as varchar)")
]

The exception for unsupported types in result columns is raised when
the query is executed, not when it is prepared; for parameters it is
raised when the parameter values are supplied. Thus even unexecutable
prepared statements can be inspected using
@racket[prepared-statement-parameter-types] and
@racket[prepared-statement-result-types].


@section[#:tag "db-types"]{SQL Type Conversions}

This section describes the correspondences between SQL types and
Racket types for the supported database systems.

@subsection[#:tag "postgresql-types"]{PostgreSQL Types}

This section applies to connections created with
@racket[postgresql-connect].

The following table lists the PostgreSQL types known to this library, using the
names listed in @tt{pg_type.typname}, along with their corresponding Racket
representations.

@centered{
@tabbing{
@bold{PostgreSQL}       @& @bold{type}             @& @bold{Racket} @//
@tt{bool}               @& @racket['boolean]       @& @racket[boolean?] @//
@tt{char}               @& @racket['char1]         @& @racket[char?] @//
@tt{int2}               @& @racket['smallint]      @& @racket[exact-integer?] @//
@tt{int4}               @& @racket['integer]       @& @racket[exact-integer?] @//
@tt{int8}               @& @racket['bigint]        @& @racket[exact-integer?] @//
@tt{float4}             @& @racket['real]          @& @racket[real?] @//
@tt{float8}             @& @racket['double]        @& @racket[real?] @//
@tt{numeric}            @& @racket['decimal]       @& @racket[real?] @//
@tt{bpchar}             @& @racket['character]     @& @racket[string?] @//
@tt{varchar}            @& @racket['varchar]       @& @racket[string?] @//
@tt{uuid}               @& @racket['uuid]          @& @racket[uuid?] @//
@tt{text}               @& @racket['text]          @& @racket[string?] @//
@tt{bytea}              @& @racket['bytea]         @& @racket[bytes?] @//
@tt{date}               @& @racket['date]          @& @racket[sql-date?] @//
@tt{time}               @& @racket['time]          @& @racket[sql-time?] @//
@tt{timetz}             @& @racket['timetz]        @& @racket[sql-time?] @//
@tt{timestamp}          @& @racket['timestamp]     @& @racket[sql-timestamp?]
                                                      or @racket[-inf.0] or @racket[+inf.0] @//
@tt{timestamptz}        @& @racket['timestamptz]   @& @racket[sql-timestamp?]
                                                      or @racket[-inf.0] or @racket[+inf.0] @//
@tt{interval}           @& @racket['interval]      @& @racket[sql-interval?] @//
@tt{bit}                @& @racket['bit]           @& @racket[bit-vector?] @//
@tt{varbit}             @& @racket['varbit]        @& @racket[bit-vector?] @//

@tt{json}               @& @racket['json]          @& @racket[jsexpr?] @//
@tt{jsonb}              @& @racket['jsonb]         @& @racket[jsexpr?] @//
@tt{int4range}          @& @racket['int4range]     @& @racket[pg-range-or-empty?] @//
@tt{int8range}          @& @racket['int8range]     @& @racket[pg-range-or-empty?] @//
@tt{numrange}           @& @racket['numrange]      @& @racket[pg-range-or-empty?] @//
@tt{tsrange}            @& @racket['tsrange]       @& @racket[pg-range-or-empty?] @//
@tt{tstzrange}          @& @racket['tstzrange]     @& @racket[pg-range-or-empty?] @//
@tt{daterange}          @& @racket['daterange]     @& @racket[pg-range-or-empty?] @//

@tt{point}              @& @racket['point]         @& @racket[point?] @//
@tt{lseg}               @& @racket['lseg]          @& @racket[line?] @//
@tt{path}               @& @racket['path]          @& @racket[pg-path?] @//
@tt{box}                @& @racket['box]           @& @racket[pg-box?] @//
@tt{polygon}            @& @racket['polygon]       @& @racket[polygon?] @//
@tt{circle}             @& @racket['circle]        @& @racket[pg-circle?]
}
}

The @racket['char1] type, written @tt{"char"} in PostgreSQL's SQL
syntax (the quotation marks are significant), is one byte, essentially
a tiny integer written as a character.

A SQL value of type @tt{decimal} is converted to either an exact
rational or @racket[+nan.0]. When converting Racket values to SQL
@tt{decimal}, exact rational values representable by finite decimal
strings are converted without loss of precision. (Precision may be
lost, of course, if the value is then stored in a database field of
lower precision.) Other real values are converted to decimals with a
loss of precision. In PostgreSQL, @tt{numeric} and @tt{decimal} refer
to the same type.

@examples[#:eval the-eval
(query-value pgc "select real '+Infinity'")
(query-value pgc "select numeric '12345678901234567890'")
]

A SQL @tt{timestamp with time zone} is converted to a Racket
@racket[sql-timestamp] in UTC---that is, with a @racket[tz] field of
@racket[0]. If a Racket @racket[sql-timestamp] without a time zone
(@racket[tz] is @racket[#f]) is given for a parameter of type
@tt{timestamp with time zone}, it is treated as a timestamp in
UTC. See also @secref["postgresql-timestamp-tz"].

The geometric types such as @racket['point] are represented by
structures defined in the @racketmodname[db/util/geometry] and
@racketmodname[db/util/postgresql] modules.

PostgreSQL user-defined @emph{domains} are supported in query results
if the underlying type is supported. Recordset headers and
@racket[prepared-statement-result-types] report them in terms of the
underlying type. Parameters with user-defined domain types are not
currently supported. As a workaround, cast the parameter to the
underlying type. For example, if the type of @tt{$1} is a domain whose
underlying type is @tt{integer}, then replace @tt{$1} with
@tt{($1::integer)}.

For each type in the table above, the corresponding
@hyperlink["http://www.postgresql.org/docs/8.2/static/arrays.html"]{array
type} is also supported, using the @racket[pg-array] structure. Use
the
@hyperlink["http://www.postgresql.org/docs/8.2/static/functions-comparisons.html#AEN14122"]{
@tt{= ANY}} syntax with an array parameter instead of dynamically
constructing a SQL @tt{IN} expression:

@examples[#:eval the-eval
(query-value pgc "select 1 in (1, 2, 3)")
(query-value pgc "select 1 = any ($1::integer[])"
             (list->pg-array (list 1 2 3)))
]

A list may be provided for an array parameter, in which case it is
automatically converted using @racket[list->pg-array]. The type
annotation can be dropped when the array type can be inferred from the
left-hand side.

@examples[#:eval the-eval
(query-value pgc "select 1 = any ($1)" (list 1 2 3))
(query-value pgc "select $1::integer = any ($2)"
             1 (list 1 2 3))
(query-value pgc "select $1 = any ($2)" (code:comment "what type are we using?")
             1 (list 1 2 3))
]

PostgreSQL defines many other types, such as network addresses and row
types. These are currently not supported, but support may be added in
future versions of this library.

@history[#:changed "1.1" @elem{Added support for the @racket['uuid] type.}]
@history[#:changed "1.14" @elem{The @tt{numeric} type now supports infinities.}]


@subsection[#:tag "mysql-types"]{MySQL Types}

This section applies to connections created with
@racket[mysql-connect].

The types that MySQL reports types for query results do not correspond exactly
to the declared types in SQL. The following table lists the MySQL result types
known to this library, along with their corresponding Racket representations:
@centered{
@tabbing{
@bold{MySQL}      @& @bold{result type}       @& @bold{Racket} @//
@tt{INTEGER}      @& @racket['integer]        @& @racket[exact-integer?] @//
@tt{TINYINT}      @& @racket['tinyint]        @& @racket[exact-integer?] @//
@tt{SMALLINT}     @& @racket['smallint]       @& @racket[exact-integer?] @//
@tt{MEDIUMINT}    @& @racket['mediumint]      @& @racket[exact-integer?] @//
@tt{BIGINT}       @& @racket['bigint]         @& @racket[exact-integer?] @//
@tt{FLOAT}        @& @racket['real]           @& @racket[real?] @//
@tt{DOUBLE}       @& @racket['double]         @& @racket[real?] @//
@tt{DECIMAL}      @& @racket['decimal]        @& @racket[exact?] @//
@tt{NUMERIC}      @& @racket['decimal]        @& @racket[exact?] @//

@tt{CHAR}         @& @racket['string] [1]     @& @racket[string?] @//
@tt{VARCHAR}      @& @racket['var-string] [1] @& @racket[string?] @//
@tt{TEXT}         @& @racket['blob] [1]       @& @racket[string?] @//

@tt{BINARY}       @& @racket['string] [1]     @& @racket[bytes?] @//
@tt{VARBINARY}    @& @racket['var-string] [1] @& @racket[bytes?] @//
@tt{BLOB}         @& @racket['blob] [1]       @& @racket[bytes?] @//
@tt{BIT}          @& @racket['bit]            @& @racket[bit-vector?] @//

@tt{DATE}         @& @racket['date]           @& @racket[sql-date?] @//
@tt{DATETIME}     @& @racket['datetime]       @& @racket[sql-timestamp?] @//
@tt{TIMESTAMP}    @& @racket['datetime]       @& @racket[sql-timestamp?] @//
@tt{TIME}         @& @racket['time] [2]       @& @racket[sql-time?] or @racket[sql-day-time-interval?] @//

@tt{JSON}         @& @racket['json] [3]       @& @racket[jsexpr?] @//
@tt{VECTOR}       @& @racket['vector] [4]     @& @racket[flvector?] @//
@tt{GEOMETRY}     @& @racket['geometry] [5]   @& @racket[geometry2d?]
}}

Historically, MySQL did not provide types for query parameters; rather, the
client was expected to tell the server the type of each parameter it
sent. (Since MySQL v8.0, the server sends generally accurate type information
for parameters, but this library currently discards that information.)
Consequently, @racket[prepared-statement-parameter-types] reports the
pseudo-type @racket['any] for every parameter of a MySQL prepared statement, and
the parameter type sent to the server is inferred from the value as follows:
@centered{
@tabbing[#:spacing 8]{
  @bold{Racket} @& @bold{parameter type} @//
  @racket[string?]   @& @racket['var-string] @//
  @racket[exact-integer?] @& @racket['integer] or @racket['bigint] or @racket['decimal] @//
  @racket[rational?] @& @racket['double] @//
  @racket[bytes?]    @& @racket['blob] @//
  @racket[sql-date?] @& @racket['date] @//
  @racket[sql-time?] @& @racket['time] @//
  @racket[sql-day-time-interval?] @& @racket['time] @//
  @racket[sql-timestamp?] @& @racket['timestamp] @//
  @racket[bit-vector?] @& @racket['bit] @//
  @racket[geometry2d?] @& @racket['blob] or @racket['geometry] [5] @//
  @racket[mysql-json?] @& @racket['json]
}}

[1] Fields of type @tt{CHAR} and @tt{BINARY} are both reported as
@racket['string]; they are distinguished by the absence or presence of
@racket['binary] in the type's flags, respectively. Likewise, @tt{VARCHAR} and
@tt{VARBINARY} are reported as @racket['var-string]; and @tt{TEXT} and @tt{BLOB}
are reported as @racket['blob]; and they are distinguished by the absence or
presence of a @racket['binary] flag. Both @tt{TEXT} and @tt{BLOB} types have the
@racket['blob] flag set.

[2] MySQL's @tt{TIME} type represents time intervals, which may not correspond
to times of day (for example, the interval may be negative or larger than 24
hours). In conversion from MySQL results to Racket values, those @tt{TIME}
values that represent times of day are converted to @racket[sql-time] values;
the rest are represented by @racket[sql-interval] values. (See also
@racket[sql-time->sql-interval] and @racket[sql-interval->sql-time].)

[3] The @tt{JSON} type (introduced in MySQL 5.7) is represented by
@racket[jsexpr?]  values, but that representation overlaps with the
representation of MySQL character types (eg, @tt{VARCHAR}), and the values are
interpreted in incompatible ways. Query parameters are only sent as @tt{JSON} if
they are wrapped with @racket[mysql-json]; @tt{JSON} result values are returned
as unwrapped @racket[jsexpr?] values.

[4] The @tt{VECTOR} type (introduced in MySQL 9.0) is represented by non-empty
@tech/reference{flvectors} (@racket[flvector?]). Beware that flvectors store
double-precision floating-point numbers, but MySQL @tt{VECTOR} fields are
single-precision.

[5] Geometry values (@racket[geometry2d?]) are sent using their ``well-known
binary'' encoding. For servers version 8.0 and later, the values are sent as
@racket['blob]; for servers version 5.7.x and earlier, values are sent as
@racket['geometry].

The MySQL @tt{enum} and @tt{set} types are not supported. As a
workaround, cast them to or from either integers or strings.

@history[#:changed "1.11" @elem{Added support for @tt{JSON}, @tt{VECTOR}.
 Changed format of @tt{GEOMETRY} query parameters for servers running MySQL 8.0
 and later.}]
@history[#:changed "1.14" @elem{Send exact integers as @tt{DECIMAL}
 instead of @tt{FLOAT}.}]


@subsection[#:tag "cassandra-types"]{Cassandra Types}

This section applies to connections created with
@racket[cassandra-connect].

The following table lists the Cassandra types known to this library,
along with their corresponding Racket representations.

@centered{
@tabbing[#:spacing 8]{
  @bold{Cassandra type}   @& @bold{Racket type} @//
  @racket['ascii]         @& @racket[string?] @//
  @racket['bigint]        @& @racket[exact-integer?] @//
  @racket['int]           @& @racket[exact-integer?] @//
  @racket['blob]          @& @racket[bytes?] @//
  @racket['boolean]       @& @racket[boolean?] @//
  @racket['decimal]       @& @racket[exact-integer?] @//
  @racket['double]        @& @racket[real?] @//
  @racket['float]         @& @racket[real?] @//
@;@racket['inet]          @& @racket[???] @//
  @racket['text]          @& @racket[string?] @//
  @racket['varchar]       @& @racket[string?] @//
  @racket['timestamp]     @& @racket[sql-timestamp?] @//
  @racket['uuid]          @& @racket[uuid?] @//
  @racket['timeuuid]      @& @racket[uuid?] @//
  @racket['varint]        @& @racket[exact-integer?] @//
  @racket[`(list ,_t)]      @& @racket[(listof _t)] @//
  @racket[`(set ,_t)]       @& @racket[(set/c _t)] @//
  @racket[`(map ,_k ,_v)]   @& @racket[(alistof _k _v)] @//
  @racket[`(tuple ,_t ...)] @& @racket[(vector/c _t ...)]
}}


@subsection[#:tag "sqlite-types"]{SQLite Types}

This section applies to connections created with
@racket[sqlite3-connect].

The following table lists the SQLite types known to this library,
along with their corresponding Racket representations.

Unlike PostgreSQL and MySQL, SQLite does not enforce declared type
constraints (with the exception of @tt{integer primary key}) on
@emph{columns}. Rather, every SQLite @emph{value} has an associated
``storage class''.

@centered{
@tabbing{
  @bold{SQLite storage class}  @& @bold{Racket type} @//
  @tt{integer}                 @& @racket[exact-integer?] @//
  @tt{real}                    @& @racket[real?] @//
  @tt{text}                    @& @racket[string?] @//
  @tt{blob}                    @& @racket[bytes?]
}
}

SQLite does not report specific parameter and result types for
prepared queries. Instead, they are assigned the pseudo-type
@racket['any]. Conversion of Racket values to parameters accepts
strings, bytes, and real numbers.

An exact integer that cannot be represented as a 64-bit signed integer
is converted as @tt{real}, not @tt{integer}.

@fake-examples[
[(expt 2 80)
 (expt 2 80)]
[(query-value slc "select ?" (expt 2 80))
 1.2089258196146292e+24]
]


@subsection[#:tag "odbc-types"]{ODBC Types}

This section applies to connections created with @racket[odbc-connect]
or @racket[odbc-driver-connect].

The following table lists the
@hyperlink["https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types"]{ODBC
types} known to this library, along with their corresponding Racket
representations.
@tabular[
#:style 'boxed
(list
(list @bold{ODBC}             @bold{type}             @bold{Racket})
(list @tt{SQL_CHAR}           @racket['character]     @racket[string?])
(list @tt{SQL_VARCHAR}        @racket['varchar]       @racket[string?])
(list @tt{SQL_LONGVARCHAR}    @racket['longvarchar]   @racket[string?])

(list @tt{SQL_WCHAR}          @racket['wchar]         @racket[string?])
(list @tt{SQL_WVARCHAR}       @racket['wvarchar]      @racket[string?])
(list @tt{SQL_WLONGVARCHAR}   @racket['wlongvarchar]  @racket[string?])

(list @tt{SQL_BINARY}         @racket['binary]        @racket[bytes?])
(list @tt{SQL_VARBINARY}      @racket['varbinary]     @racket[bytes?])
(list @tt{SQL_LONGVARBINARY}  @racket['longvarbinary] @racket[bytes?])

(list @tt{SQL_INTEGER}        @racket['integer]       @racket[exact-integer?])
(list @tt{SQL_TINYINT}        @racket['tinyint]       @racket[exact-integer?])
(list @tt{SQL_SMALLINT}       @racket['smallint]      @racket[exact-integer?])
(list @tt{SQL_BIGINT}         @racket['bigint]        @racket[exact-integer?])
(list @tt{SQL_FLOAT}          @racket['float]         @racket[real?])
(list @tt{SQL_REAL}           @racket['real]          @racket[real?])
(list @tt{SQL_DOUBLE}         @racket['double]        @racket[real?])
(list @tt{SQL_NUMERIC}        @racket['numeric]       @racket[rational?])
(list @tt{SQL_DECIMAL}        @racket['decimal]       @racket[rational?])

(list @tt{SQL_BIT}            @racket['bit1]          @racket[boolean?])

(list @tt{SQL_DATE}           @racket['date]          @racket[sql-date?])
(list @tt{SQL_TIME}           @racket['time]          @racket[sql-time?])
(list @tt{SQL_DATETIME}       @racket['datetime]      @racket[sql-timestamp?])
(list @tt{SQL_TIMESTAMP}      @racket['timestamp]     @racket[sql-timestamp?])

(list @tt{SQL_UNKNOWN}        @racket['unknown]       @racket[string?])

(list @bold{SQL Server extensions:} 'cont 'cont)
(list @tt{SQL_SS_TIME2 (-154)}      @racket['time2]          @racket[sql-time?])

(list @bold{DB2 extensions:}        'cont 'cont)
(list @tt{SQL_GRAPHIC (-95)}        @racket['graphic]        @racket[string?])
(list @tt{SQL_VARGRAPHIC (-96)}     @racket['vargraphic]     @racket[string?])
(list @tt{SQL_LONGVARGRAPHIC (-97)} @racket['longvargraphic] @racket[string?])
(list @tt{SQL_BLOB (-98)}           @racket['blob]           @racket[bytes?])
(list @tt{SQL_CLOB (-99)}           @racket['clob]           @racket[string?])
(list @tt{SQL_DBCLOB (-350)}        @racket['dbclob]         @racket[string?]))
]

If the connection was made with @racket[#:strict-parameter-types? #t], then the
parameter type set by the ODBC driver is usually respected. Unfortunately, few
ODBC drivers provide useful parameter types for prepared queries. Some assign
parameters the @racket['unknown] type; worse, some assign all parameters a
single type such as @racket['varchar]. To avoid enforcing irrelevant type
constraints in the latter case, connect with @racket[#:strict-parameter-types?
#f], and the connection will assign all parameters the type
@racket['unknown]. When a parameter type is @racket['unknown], this library
sends the ODBC driver a parameter type based on the parameter value as follows:
@tabular[
#:style 'boxed
(list
(list @bold{Racket}           @bold{parameter type})
(list @racket[exact-integer?] @elem{@tt{SQL_INTEGER} or @tt{SQL_BIGINT} or @tt{SQL_NUMERIC}})
(list @racket[(and/c real? exact?)]     @elem{@tt{SQL_NUMERIC} or @tt{SQL_DOUBLE}})
(list @racket[(and/c real? inexact?)]   @tt{SQL_DOUBLE})
(list @racket[boolean?]       @tt{SQL_BIT})
(list @racket[string?]        @elem{@tt{SQL_VARCHAR} or @tt{SQL_WVARCHAR} [1]})
(list @racket[bytes?]         @elem{@tt{SQL_VARBINARY} [2]})
(list @racket[sql-date?]      @tt{SQL_DATE})
(list @racket[sql-time?]      @elem{@tt{SQL_TIME}})
(list @racket[sql-day-time-interval?] @tt{SQL_TIME})
(list @racket[sql-timestamp?] @tt{SQL_TIMESTAMP})
)]

[1] The type of a string parameter is determined by the connection's character
mode; see @racket[odbc-connect]'s @racket[#:character-mode] argument. Depending
on connection quirks, the type may also be set as @tt{SQL_LONGVARCHAR} or
@tt{SQL_WLONGVARCHAR}.

[2] Depending on connection quirks, the type may also be set as
@tt{SQL_LONGVARBINARY}.

Interval types are not currently supported for ODBC connections.

@history[#:changed "1.14" @elem{Added support for the DB2 types. Exact numbers
  are sent as @tt{SQL_NUMERIC} if possible.}]

@;{----------------------------------------}

@section[#:tag "sql-data"]{SQL Data}

This section describes data types for representing various SQL types
that have no existing appropriate counterpart in Racket.

@subsection{SQL NULL}

SQL @tt{NULL} is translated into the unique @racket[sql-null] value.

@defthing[sql-null sql-null?]{

  A special value used to represent @tt{NULL} values in query
  results. The @racket[sql-null] value may be recognized using
  @racket[eq?].

@examples[#:eval the-eval
(query-value pgc "select NULL")
]
}

@defproc[(sql-null? [x any/c]) boolean?]{

  Returns @racket[#t] if @racket[x] is @racket[sql-null]; @racket[#f]
  otherwise.
}

@defproc[(sql-null->false [x any/c]) any/c]{

  Returns @racket[#f] if @racket[x] is @racket[sql-null]; otherwise
  returns @racket[x].

@examples[#:eval the-eval
(sql-null->false "apple")
(sql-null->false sql-null)
(sql-null->false #f)
]
}

@defproc[(false->sql-null [x any/c]) any/c]{

  Returns @racket[sql-null] if @racket[x] is @racket[#f]; otherwise
  returns @racket[x].

@examples[#:eval the-eval
(false->sql-null "apple")
(false->sql-null #f)
]
}


@subsection{Dates and Times}

The @tt{DATE}, @tt{TIME} (@tt{WITH TIME ZONE} and without),
@tt{TIMESTAMP} (@tt{WITH TIME ZONE} and without), and @tt{INTERVAL}
SQL types are represented by the following structures.

See also @secref["datetime-util"] for more functions on datetime
values.

@defstruct*[sql-date
            ([year exact-integer?]
             [month (integer-in 0 12)]
             [day (integer-in 0 31)])]{

  Represents a SQL date.

  Dates with zero-valued @racket[month] or @racket[day] components are
  a MySQL extension.
}

@defstruct*[sql-time
            ([hour exact-nonnegative-integer?]
             [minute exact-nonnegative-integer?]
             [second exact-nonnegative-integer?]
             [nanosecond exact-nonnegative-integer?]
             [tz (or/c exact-integer? #f)])]
@defstruct*[sql-timestamp
            ([year exact-nonnegative-integer?]
             [month exact-nonnegative-integer?]
             [day exact-nonnegative-integer?]
             [hour exact-nonnegative-integer?]
             [minute exact-nonnegative-integer?]
             [second exact-nonnegative-integer?]
             [nanosecond exact-nonnegative-integer?]
             [tz (or/c exact-integer? #f)])]{

  Represents SQL times and timestamps.

  The @racket[tz] field indicates the time zone offset as the number
  of seconds east of GMT (as in @racketmodname[srfi/19
  #:indirect]). If @racket[tz] is @racket[#f], the time or timestamp
  does not carry time zone information.

  The @racket[sql-time] and @racket[sql-timestamp] structures store
  fractional seconds to nanosecond precision for compatibility with
  @racketmodname[srfi/19 #:indirect]. Note, however, that database
  systems generally do not support nanosecond precision; PostgreSQL,
  for example, only supports microsecond precision.

@examples[#:eval the-eval
(query-value pgc "select date '25-dec-1980'")
(query-value pgc "select time '7:30'")
(query-value pgc "select timestamp 'epoch'")
(query-value pgc "select timestamp with time zone 'epoch'")
]
}

@defstruct*[sql-interval
            ([years exact-integer?]
             [months exact-integer?]
             [days exact-integer?]
             [hours exact-integer?]
             [minutes exact-integer?]
             [seconds exact-integer?]
             [nanoseconds exact-integer?])]{

  Represents lengths of time. Intervals are normalized to satisfy the
  following constraints:
  @itemlist[
  @item{@racket[years] and @racket[months] have the same sign}
  @item{@racket[months] ranges from @racket[-11] to @racket[11]}
  @item{@racket[days], @racket[hours], @racket[minutes],
    @racket[seconds], and @racket[nanoseconds] all have the same sign}
  @item{@racket[hours] ranges from @racket[-23] to @racket[23]}
  @item{@racket[minutes] and @racket[seconds] range from @racket[-59]
    to @racket[59]} 
  @item{@racket[nanoseconds] ranges from
    @racket[(- (sub1 #, @racketvalfont{#e1e9}))] to
    @racket[(sub1 #, @racketvalfont{#e1e9})]}
  ]

  That is, an interval consists of two groups of components:
  year-month and day-time, and normalization is done only within
  groups. In fact, the SQL standard recognizes those two types of
  intervals separately (see @racket[sql-year-month-interval?] and
  @racket[sql-day-time-interval?], below), and does not permit
  combining them. Intervals such as @tt{1 month 3 days} are a
  PostgreSQL extension.
}

@defproc[(sql-year-month-interval? [x any/c]) boolean?]{

  Returns @racket[#t] if @racket[x] is a @racket[sql-interval] value
  where the @racket[days], @racket[hours], @racket[minutes],
  @racket[seconds], and @racket[nanoseconds] fields are zero.
}

@defproc[(sql-day-time-interval? [x any/c]) boolean?]{

  Returns @racket[#t] if @racket[x] is a @racket[sql-interval] value
  where the @racket[years] and @racket[months] fields are zero.
}

@defproc[(sql-interval->sql-time [interval sql-interval?]
                                 [failure any/c (lambda () (error ....))])
         any]{

  If @racket[interval] is a day-time interval that
  represents a time of day, returns the corresponding
  @racket[sql-time] value. In particular, the following must be true:
  @itemlist[
  @item{@racket[hours], @racket[minutes], @racket[seconds], and
    @racket[nanoseconds] must all be non-negative}
  @item{@racket[hours] must be between @racket[0] and @racket[23]}
  ]
  The corresponding constraints on @racket[minutes], etc are
  enforced by the constructor.

  If @racket[interval] is out of range, the @racket[failure] value is
  called, if it is a procedure, or returned, otherwise.
}

@defproc[(sql-time->sql-interval [time sql-time?])
         sql-day-time-interval?]{

  Converts @racket[time] to an interval. If @racket[time] has
  time-zone information, it is ignored.
}


@subsection{Bits}

The @tt{BIT} and @tt{BIT VARYING} (@tt{VARBIT}) SQL types are
represented by bit-vectors (@racketmodname[data/bit-vector #:indirect]).

The following functions are provided for backwards compatibility. They
are deprecated and will be removed in a future release of Racket.

@deftogether[[
@defproc[(make-sql-bits [len exact-nonnegative-integer?]) 
         sql-bits?]
@defproc[(sql-bits? [v any/c]) boolean?]
@defproc[(sql-bits-length [b sql-bits?])
         exact-nonnegative-integer?]
@defproc[(sql-bits-ref [b sql-bits?] [i exact-nonnegative-integer?])
         boolean?]
@defproc[(sql-bits-set! [b sql-bits?] 
                        [i exact-nonnegative-integer?] 
                        [v boolean?])
         void?]
@defproc[(sql-bits->list [b sql-bits?]) (listof boolean?)]
@defproc[(sql-bits->string [b sql-bits?]) string?]
@defproc[(list->sql-bits [lst (listof boolean?)]) sql-bits?]
@defproc[(string->sql-bits [s string?]) sql-bits?]]]{

Deprecated; use @racketmodname[data/bit-vector #:indirect] instead.

}

@(close-eval the-eval)
