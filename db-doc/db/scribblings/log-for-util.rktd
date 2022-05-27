;; This file was created by make-log-based-eval
((require racket/class db db/util/postgresql db/util/datetime)
 ((3) 0 () 0 () () (c values c (void)))
 #""
 #"")
((begin
   (define pgc (dsn-connect 'db-scribble-env))
   (query-exec
    pgc
    "create temporary table the_numbers (n integer, d varchar(20))")
   (query-exec pgc "insert into the_numbers values (0, 'nothing')")
   (query-exec
    pgc
    "insert into the_numbers values (1, 'the loneliest number')")
   (query-exec pgc "insert into the_numbers values (2, 'company')")
   (query-exec pgc "insert into the_numbers values (3, 'a crowd')"))
 ((3) 0 () 0 () () (c values c (void)))
 #""
 #"")
((sql-datetime->srfi-date (query-value pgc "select time '7:30'"))
 ((3) 0 () 0 () () (c values c (date* 0 30 7 1 1 0 0 0 #f 0 0 "")))
 #""
 #"")
((sql-datetime->srfi-date (query-value pgc "select date '25-dec-1980'"))
 ((3) 0 () 0 () () (c values c (date* 0 0 0 25 12 1980 4 359 #f 0 0 "")))
 #""
 #"")
((sql-datetime->srfi-date (query-value pgc "select timestamp 'epoch'"))
 ((3) 0 () 0 () () (c values c (date* 0 0 0 1 1 1970 4 0 #f 0 0 "")))
 #""
 #"")
((define cidr-typeid
   (query-value pgc "select oid from pg_type where typname = $1" "cidr"))
 ((3) 0 () 0 () () (c values c (void)))
 #""
 #"")
(cidr-typeid ((3) 0 () 0 () () (q values 650)) #"" #"")
((send pgc add-custom-types
   (list
    (pg-custom-type cidr-typeid 'cidr #:recv bytes->list #:send list->bytes)))
 ((3) 0 () 0 () () (c values c (void)))
 #""
 #"")
((query-value pgc "select cidr '127.0.0.0/24'")
 ((3) 0 () 0 () () (q values (2 24 1 4 127 0 0 0)))
 #""
 #"")
