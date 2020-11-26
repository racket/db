(
 ;; ----------------------------------------
 ;; PostgreSQL

 (pg (db postgresql
         (#:database "rkt" #:user "rkt" #:password "rktpwd" #:port 5432)
         ((db:test (ispg pg92)))))
 (pgssl (db postgresql
            (#:database "rkt" #:user "rkt" #:password "rktpwd" #:port 5432 #:ssl yes)
            ((db:test (ispg pg92 ssl)))))

 ;; ----------------------------------------
 ;; MySQL

 ;; For testing server with caching-sha2-password, use 'connect-first-ssl to
 ;; "warm up" the sha2 cache, but then it runs the rest of the tests w/o SSL.
 (my (db mysql
         (#:port 3306 #:database "rkt" #:user "rkt" #:password "rktpwd")
         ((db:test (ismy my:json connect-first-ssl)))))
 (myssl (db mysql
            (#:port 3306 #:database "rkt" #:user "rkt" #:password "rktpwd" #:ssl yes)
            ((db:test (ismy my:json ssl)))))

 ;; ----------------------------------------
 ;; ODBC

 ;; Oracle ODBC
 ;; Note: needs LD_LIBRARY_PATH to point to appropriate ODBC libs.
 ;; Tests sometimes spuriously fail if connections are made to
 ;; quickly; use sleep-before-connect to work around.
 (ora11 (db odbc-driver
            ("DRIVER=Oracle 11g ODBC driver;DBQ=localhost:1521/XE;UID=system;PWD=orapwd"
             #:quirks (no-c-bigint))
            ((db:test (isora sleep-before-connect)))))
 (ora19 (db odbc-driver
            ("DRIVER=Oracle 19 ODBC driver;DBQ=localhost:1521/XE;UID=system;PWD=orapwd"
             #:quirks (no-c-bigint))
            ((db:test (isora sleep-before-connect)))))

 ;; DB2 ODBC
 ;; Note: needs LD_LIBRARY_PATH to point to appropriate ODBC libs.
 ;; Note: tests with keep-unused connection <1s; without ~150s
 (db2 (db odbc-driver
          ("DRIVER=Db2;Database=testdb;Hostname=localhost;Port=50000;UID=db2inst1;PWD=db2pwd"
           #:quirks (no-c-numeric))
          ((db:test (isdb2 keep-unused-connection)))))

 ;; MS SQL Server ODBC
 (mss (db odbc-driver
          ;; Add "MARS_Connection=yes" to support multiple simultaneous open
          ;; statements, but it causes a ~15x (!!) slowdown for the test suite.
          ("DRIVER=FreeTDS;Server=localhost;Port=1433;UID=sa;PWD=abcdEFGH89"
           ;; Unlike MS drivers on win32, FreeTDS seems to need no-c-numeric.
           #:quirks (no-c-numeric))
          ((db:test (ismss)))))
 )
