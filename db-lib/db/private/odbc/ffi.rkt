#lang racket/base
(require ffi/unsafe
         ffi/unsafe/define
         "ffi-constants.rkt")
(provide (all-from-out "ffi-constants.rkt"))
(provide (protect-out (all-defined-out)))

(define-cpointer-type _sqlhandle)

(define-cpointer-type _sqlhenv)
(define-cpointer-type _sqlhdbc)
(define-cpointer-type _sqlhstmt)
(define-cpointer-type _sqlhdesc)

(define _sqllen _long)
(define _sqlulen _ulong)

;; https://docs.microsoft.com/en-us/sql/odbc/reference/odbc-64-bit-information
(define sizeof-SQLLONG 4) ;; yes, even on 64-bit environments
(define sizeof-SQLLEN (ctype-sizeof _sqllen))

(define _sqlsmallint _sshort)
(define _sqlusmallint _ushort)
(define _sqlinteger _sint)
(define _sqluinteger _uint)
(define _sqlreturn _sqlsmallint)

;; For dealing with param buffers, which must not be moved by GC

;; bytes->non-moving-pointer : Bytes -> NonMovingPointer
(define (bytes->non-moving-pointer bs)
  (define len (bytes-length bs))
  ;; Note: avoid (malloc 0); returns #f, SQL Server driver treats as SQL NULL!
  (define copy (malloc (max 1 len) 'atomic-interior))
  (memcpy copy bs len)
  copy)

;; cpstr{2,4} : String -> Bytes
;; Converts string to utf16/ucs4 (platform-endian) bytes.
(define (cpstr2 s)
  (string->bytes* s "platform-UTF-16" "platform-UTF-8"))
(define (cpstr4 s)
  (string->bytes* s (if (system-big-endian?) "UTF-32BE" "UTF-32LE") "UTF-8"))

;; mkstr{2,4} : Bytes Nat _ -> String
;; Converts utf16/ucs4 (platform-endian) to string.
(define (mkstr2 buf [len (bytes-length buf)] [fresh? #f])
  (bytes->string* buf len "platform-UTF-16" "platform-UTF-8"))
(define (mkstr4 buf [len (bytes-length buf)] [fresh? #f])
  (bytes->string* buf len (if (system-big-endian?) "UTF-32BE" "UTF-32LE") "UTF-8"))

;; bytes->string* : Bytes String -> String
(define (bytes->string* b len benc [senc "UTF-8"])
  (define conv (bytes-open-converter benc senc))
  (define-values (b* nconv status) (bytes-convert conv b 0 len))
  (bytes-close-converter conv)
  (case status
    [(complete)
     (bytes->string/utf-8 b*)]
    [else
     (error 'bytes->string* "invalid ~a encoding\n  bytes: ~e" b)]))

;; string->bytes* : String String -> Bytes
(define (string->bytes* s benc [senc "UTF-8"])
  (define b (string->bytes/utf-8 s))
  (define conv (bytes-open-converter senc benc))
  (define-values (b* nconv status) (bytes-convert conv b))
  (bytes-close-converter conv)
  (case status
    [(complete)
     b*]
    [else
     (error 'string->bytes* "unable to convert to ~a\n  string: ~e" s)]))

;; ========================================

#|
Docs at http://msdn.microsoft.com/en-us/library/ms712628%28v=VS.85%29.aspx
|#

(define-values (odbc-lib WCHAR-SIZE)
  (case (system-type)
    ((windows)
     ;; Windows ODBC defines wchar_t (thus WCHAR, thus SQLWCHAR) as 16-bit
     (values (ffi-lib "odbc32.dll" #:fail (lambda () #f))
             2))
    ((macosx)
     ;; Mac OS uses iodbc, which defines SQLWCHAR as wchar_t, as 32-bit
     (values (ffi-lib "libiodbc" '("2" #f) #:fail (lambda () #f))
             4))
    ((unix)
     (cond [(member (path->string (system-library-subpath #f))
                    '("i386-openbsd" "x86_64-openbsd"))
            ;; OpenBSD uses iodbc
            (values (ffi-lib "libiodbc" '("3.16" #f) #:fail (lambda () #f))
                    4)]
           [else
            ;; Other unixes use unixodbc, which defines WCHAR as 16-bit
            ;; for compat w/ Windows (even though Linux wchar_t is 32-bit)
            (values (ffi-lib "libodbc" '("2" "1" #f) #:fail (lambda () #f))
                    2)]))))

(define-ffi-definer define-odbc odbc-lib
  #:default-make-fail make-not-available)

(define (ok-status? n)
  (or (= n SQL_SUCCESS)
      (= n SQL_SUCCESS_WITH_INFO)))

(define-odbc SQLAllocHandle
  (_fun (type : _sqlsmallint)
        (parent : _sqlhandle/null)
        (handle : (_ptr o _sqlhandle/null))
        -> (status : _sqlreturn)
        -> (values status
                   (cond [handle
                          (cpointer-push-tag! handle
                                              (cond [(= type SQL_HANDLE_ENV) sqlhenv-tag]
                                                    [(= type SQL_HANDLE_DBC) sqlhdbc-tag]
                                                    [(= type SQL_HANDLE_STMT) sqlhstmt-tag]
                                                    [else sqlhandle-tag]))
                          handle]
                         [else handle]))))

;; SQLSetEnvAttr
;; must set odbc version env attr before making connection

(define-odbc SQLSetEnvAttr
  (_fun (env : _sqlhenv)
        (attr : _sqlinteger)
        (value-buf : _sqlinteger) ;; (the one case we care about takes int, not ptr)
        (_sqlinteger = 0)
        -> _sqlreturn))

(define-odbc SQLGetInfo
  (_fun (handle info) ::
        (handle : _sqlhdbc)
        (info : _sqlusmallint)
        (value : (_ptr o _sqluinteger)) ;; the one case we care about is uint, not char
        (0 : _sqlsmallint)
        (#f : _pointer)
        -> (status : _sqlreturn)
        -> (values status value)))

(define-odbc SQLGetInfo-string
  (_fun (handle info) ::
        (handle : _sqlhdbc)
        (info : _sqlusmallint)
        (value : _bytes = (make-bytes 250))
        (250 : _sqlsmallint)
        (len : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        (bytes->string/utf-8 value #f 0 len))))
  #:c-id SQLGetInfo)


(define-odbc SQLGetFunctions
  (_fun (handle : _sqlhdbc)
        (function-id : _sqlusmallint)
        (supported? : (_ptr o _sqlusmallint))
        -> (status : _sqlreturn)
        -> (values status (positive? supported?))))

(define-odbc SQLConnect
  (_fun (handle server user auth) ::
        (handle : _sqlhdbc)
        (server : _string)
        ((string-utf-8-length server) : _sqlsmallint)
        (user : _string)
        ((if user (string-utf-8-length user) 0) : _sqlsmallint)
        (auth : _string)
        ((if auth (string-utf-8-length auth) 0) : _sqlsmallint)
        -> _sqlreturn))

(define-odbc SQLDriverConnect
  (_fun (handle connection driver-completion) ::
        (handle : _sqlhdbc)
        (#f : _pointer)
        (connection : _string)
        ((if connection (string-utf-8-length connection) 0) : _sqlsmallint)
        (#f : _bytes)
        (0 : _sqlsmallint)
        (out-length : (_ptr o _sqlsmallint))
        (driver-completion : _sqlusmallint)
        -> (status : _sqlreturn)
        -> status))

(define-odbc SQLBrowseConnect
  (_fun (handle in-conn-string) ::
        (handle : _sqlhdbc)
        (in-conn-string : _string)
        ((if in-conn-string (string-utf-8-length in-conn-string) 0) : _sqlsmallint)
        (out-buf : _bytes = (make-bytes 1024))
        ((bytes-length out-buf) : _sqlsmallint)
        (out-len : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        (bytes->string/utf-8 out-buf #f 0 out-len)))))

(define-odbc SQLDataSources
  (_fun (handle direction server-buf descr-buf) ::
        (handle : _sqlhenv)
        (direction : _sqlusmallint)
        (server-buf : _bytes)
        ((bytes-length server-buf) : _sqlsmallint)
        (server-length : (_ptr o _sqlsmallint))
        (descr-buf : _bytes)
        ((bytes-length descr-buf) : _sqlsmallint)
        (descr-length : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        (bytes->string/utf-8 server-buf #f 0 server-length))
                   (and (ok-status? status)
                        (bytes->string/utf-8 descr-buf #f 0 descr-length)))))

(define-odbc SQLDrivers
  (_fun (handle direction driver-buf attrs-buf) ::
        (handle : _sqlhenv)
        (direction : _sqlusmallint)
        (driver-buf : _bytes)
        ((bytes-length driver-buf) : _sqlsmallint)
        (driver-length : (_ptr o _sqlsmallint))
        (attrs-buf : _bytes)
        ((if attrs-buf (bytes-length attrs-buf) 0) : _sqlsmallint)
        (attrs-length : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (if (ok-status? status)
               (values status
                       (bytes->string/utf-8 driver-buf #f 0 driver-length)
                       attrs-length)
               (values status #f #f))))

(define-odbc SQLPrepare
  (_fun (handle stmt) ::
        (handle : _sqlhstmt)
        (stmt : _string)
        ((string-utf-8-length stmt) : _sqlinteger)
        -> _sqlreturn))

(define-odbc SQLBindParameter
  (_fun (handle param-num c-type sql-type column-size digits value len len-or-ind) ::
        (handle : _sqlhstmt)
        (param-num : _sqlusmallint)
        (_sqlsmallint = SQL_PARAM_INPUT)
        (c-type : _sqlsmallint)
        (sql-type : _sqlsmallint)
        (column-size : _sqlulen)
        (digits : _sqlsmallint)
        (value : _pointer) ;; must be pinned until after SQLExecute called
        (len : _sqllen) ;; ignored for fixed-length data
        (len-or-ind : _pointer) ;; _sqllen-pointer)
        -> _sqlreturn))

(define-odbc SQLExecute
  (_fun (handle : _sqlhstmt)
        -> _sqlreturn))

(define-odbc SQLNumParams
  (_fun (handle : _sqlhstmt)
        (count : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status count)))

(define-odbc SQLDescribeParam
  (_fun (handle : _sqlhstmt)
        (parameter : _sqlusmallint)
        (data-type : (_ptr o _sqlsmallint))
        (size : (_ptr o _sqlulen))
        (digits : (_ptr o _sqlsmallint))
        (nullable : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status data-type size digits nullable)))

(define-odbc SQLNumResultCols
  (_fun (handle : _sqlhstmt)
        (count : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status count)))

(define-odbc SQLDescribeCol
  (_fun (handle column column-buf) ::
        (handle : _sqlhstmt)
        (column : _sqlusmallint)
        (column-buf : _bytes)
        (_sqlsmallint = (if column-buf (bytes-length column-buf) 0))
        (column-len : (_ptr o _sqlsmallint))
        (data-type : (_ptr o _sqlsmallint))
        (size : (_ptr o _sqlulen))
        (digits : (_ptr o _sqlsmallint))
        (nullable : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        column-buf
                        ;; Oracle returns garbage column name/len for TIME columns
                        (<= 0 column-len (bytes-length column-buf))
                        (bytes->string/utf-8 column-buf #f 0 column-len))
                   data-type size digits nullable)))

(define-odbc SQLFetch
  (_fun _sqlhstmt
        -> _sqlreturn))

(define-odbc SQLGetData
  (_fun (handle column target-type buffer start) ::
        (handle : _sqlhstmt)
        (column : _sqlusmallint)
        (target-type : _sqlsmallint)
        ((ptr-add buffer start) : _gcpointer)
        ((- (bytes-length buffer) start) : _sqllen)
        (len-or-ind : (_ptr o _sqllen))
        -> (status : _sqlreturn)
        -> (values status len-or-ind)))

(define-odbc SQLGetStmtAttr/HDesc
  (_fun (handle attr) ::
        (handle : _sqlhstmt)
        (attr :   _sqlinteger)
        (valptr : (_ptr o _sqlhdesc))
        (buflen : _sqlinteger = 0)
        (strlen : _pointer = #f)
        -> (status : _sqlreturn)
        -> (and (ok-status? status) valptr))
  #:c-id SQLGetStmtAttr)

(define-odbc SQLSetDescField/SmallInt
  (_fun (handle recno fieldid intval) ::
        (handle  : _sqlhdesc)
        (recno   : _sqlsmallint)
        (fieldid : _sqlsmallint)
        (intval  : _intptr)  ;; declared SQLPOINTER; cast
        (buflen : _sqlinteger = SQL_IS_SMALLINT)
        -> (status : _sqlreturn))
  #:c-id SQLSetDescField)

(define-odbc SQLSetDescField/Ptr
  (_fun (handle recno fieldid ptrval buflen) ::
        (handle  : _sqlhdesc)
        (recno   : _sqlsmallint)
        (fieldid : _sqlsmallint)
        (ptrval  : _pointer)  ;; declared SQLPOINTER; cast
        (buflen : _sqlinteger)
        -> (status : _sqlreturn))
  #:c-id SQLSetDescField)

(define-odbc SQLFreeStmt
  (_fun (handle : _sqlhstmt)
        (option : _sqlusmallint)
        -> _sqlreturn))

(define-odbc SQLCloseCursor
  (_fun (handle : _sqlhstmt)
        -> _sqlreturn))

(define-odbc SQLDisconnect
  (_fun (handle : _sqlhdbc)
        -> _sqlreturn))

(define-odbc SQLFreeHandle
  (_fun (handle-type : _sqlsmallint)
        (handle : _sqlhandle)
        -> _sqlreturn))

(define-odbc SQLGetDiagRec
  (_fun (handle-type handle rec-number) ::
        (handle-type : _sqlsmallint)
        (handle : _sqlhandle)
        (rec-number : _sqlsmallint)
        (sql-state-buf : _bytes = (make-bytes 6))
        (native-errcode : (_ptr o _sqlinteger))
        (message-buf : _bytes = (make-bytes 1024))
        ((bytes-length message-buf) : _sqlsmallint)
        (message-len : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        (bytes->string/utf-8 sql-state-buf #\? 0 5))
                   native-errcode
                   (and (ok-status? status)
                        (bytes->string/utf-8 message-buf #\? 0 message-len)))))

(define-odbc SQLEndTran
  (_fun (handle completion-type) ::
        (_sqlsmallint = SQL_HANDLE_DBC)
        (handle : _sqlhandle)
        (completion-type : _sqlsmallint)
        -> _sqlreturn))

(define-odbc SQLGetConnectAttr
  (_fun (handle attr) ::
        (handle : _sqlhdbc)
        (attr : _sqlinteger)
        (value : (_ptr o _sqluinteger)) ;; the attrs we care about have uint value
        (buflen : _sqlinteger = 0) ;; ignored
        (#f : _pointer)
        -> (status : _sqlreturn)
        -> (values status value)))

(define-odbc SQLSetConnectAttr
  (_fun (handle attr value) ::
        (handle : _sqlhdbc)
        (attr : _sqlinteger)
        (value : _sqluinteger) ;; the attrs we care about have uint value
        (_sqlinteger = 0)
        -> _sqlreturn))

(define-odbc SQLTables
  (_fun (handle catalog schema table) ::
        (handle : _sqlhstmt)
        (catalog : _string)
        (_sqlsmallint = (if catalog (string-utf-8-length catalog) 0))
        (schema : _string)
        (_sqlsmallint = (if schema (string-utf-8-length schema) 0))
        (table : _string)
        (_sqlsmallint = (if table (string-utf-8-length table) 0))
        (_bytes = #f)
        (_sqlsmallint = 0)
        -> _sqlreturn))
