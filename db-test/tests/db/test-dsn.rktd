(
 ;; ============================================================

 ;; The following entries support testing on servers managed by
 ;; the bin/docker-util.sh script.

 ;; ----------------------------------------
 ;; PostgreSQL

 (pg
  (db postgresql
      (#:database "rkt" #:user "rkt" #:password "rktpwd" #:port 5432)
      ((db:test (ispg pg92)))))
 (pgssl
  (db postgresql
      (#:database "rkt" #:user "rkt" #:password "rktpwd" #:port 5432 #:ssl yes)
      ((db:test (ispg pg92 ssl)))))

 ;; ----------------------------------------
 ;; MySQL

 ;; For testing server with caching-sha2-password, use 'connect-first-ssl to
 ;; "warm up" the sha2 cache, but then it runs the rest of the tests w/o SSL.
 (my
  (db mysql
      (#:port 3306 #:database "rkt" #:user "rkt" #:password "rktpwd")
      ((db:test (ismy my:json my:vector connect-first-ssl)))))
 (myssl
  (db mysql
      (#:port 3306 #:database "rkt" #:user "rkt" #:password "rktpwd" #:ssl yes)
      ((db:test (ismy my:json my:vector ssl)))))

 ;; ----------------------------------------
 ;; ODBC

 ;; MS SQL Server ODBC
 ;; https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute
 (mss17
  (db odbc-driver
      ("DRIVER=ODBC Driver 17 for SQL Server;Server=localhost;Port=1433;UID=sa;PWD=abcdEFGH89!;Encrypt=no;MARS_Connection=yes"
       #:quirks ())
      ((db:test (ismss mss17)))))
 (mss18
  (db odbc-driver
      ("DRIVER=ODBC Driver 18 for SQL Server;Server=localhost;Port=1433;UID=sa;PWD=abcdEFGH89!;Encrypt=no;MARS_Connection=yes"
       #:quirks ())
      ((db:test (ismss mss18)))))

 ;; MS SQL Server ODBC via FreeTDS
 (mssf
  (db odbc-driver
      ;; Add "MARS_Connection=yes" to support multiple simultaneous open statements,
      ;; but one interleaving test still fails unless 'no-interleave flag set.
      ("DRIVER=FreeTDS;Server=localhost;Port=1433;UID=sa;PWD=abcdEFGH89!"
       #:quirks (no-c-numeric string-longvarchar-always bytes-longvarbinary-always))
      ((db:test (ismss freetds no-interleave)))))

 ;; Oracle ODBC
 ;; Note: needs LD_LIBRARY_PATH to point to appropriate ODBC libs.
 ;; Note: needs NLS_LANG=AMERICAN_AMERICA.AL32UTF8 (or similar); see ODBC varchar tests
 (ora21
  (db odbc-driver
      ("DRIVER=Oracle 21 ODBC driver;DBQ=localhost:1521/FREE;UID=system;PWD=orapwd"
       #:quirks (no-get-c-binary))
      ((db:test (isora)))))
 (ora23
  (db odbc-driver
      ("DRIVER=Oracle 23 ODBC driver;DBQ=localhost:1521/FREE;UID=system;PWD=orapwd"
       #:quirks (no-get-c-binary))
      ((db:test (isora)))))

 ;; DB2 ODBC
 (db2
  (db odbc-driver
      ("DRIVER=Db2;Database=testdb;Hostname=localhost;Port=50000;UID=db2inst1;PWD=db2pwd"
       #:quirks (no-c-numeric))
      ((db:test (isdb2)))))

 ;; Firebird ODBC
 (fbd
  (db odbc-driver
      ("DRIVER=Firebird;UID=rkt;PWD=rktpwd;DBNAME=localhost/3050:/var/lib/firebird/data/testdb.fdb"
       #:quirks () #:character-mode utf-8)
      ((db:test (isfbd)))))

 ;; ============================================================

 ;; The following entries support testing ODBC configurations
 ;; using DSNs registered with the system's ODBC manager.

 ;; MS SQL Server
 (odbc-sqlserver
  (db odbc
      (#:dsn "test-sqlserver"
       #:character-mode wchar
       #:quirks ())
      ((db:test (ismss)))))

 ;; Oracle
 (odbc-oracle
  (db odbc
      (#:dsn "test-oracle"
       #:character-mode wchar
       #:quirks (no-c-bigint no-get-c-binary))
      ((db:test (isora)))))

 ;; DB2
 (odbc-db2
  (db odbc
      (#:dsn "test-db2"
       #:character-mode wchar
       #:quirks (no-c-numeric))
      ((db:test (isdb2)))))

 ;; Firebird
 (odbc-firebird
  (db odbc
      (#:dsn "test-firebird"
       #:character-mode utf-8
       #:quirks ())
      ((db:test (isfbd)))))

 ;; MS Access
 (odbc-access
  (db odbc
      (#:dsn "test-access"
       #:character-mode wchar
       #:quirks (char-preflight-length
                 char-precision
                 string-longvarchar-256
                 binary-precision
                 bytes-longvarbinary-256))
      ((db:test (isacc)))))

 ;; template
 (odbc-template
  (db odbc
      (#:dsn "test-odbc"
       #:character-mode wchar
       #:quirks ())
      ((db:test (is???)))))
 )
