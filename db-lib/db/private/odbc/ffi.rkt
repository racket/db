#lang racket/base
(require (for-syntax racket/base racket/syntax)
         racket/stxparam
         ffi/unsafe
         ffi/unsafe/define
         ffi/winapi
         "ffi-constants.rkt")
(provide (all-from-out "ffi-constants.rkt"))
(provide (protect-out (all-defined-out)))

(define-cpointer-type _sqlhandle)

(define-cpointer-type _sqlhenv)
(define-cpointer-type _sqlhdbc)
(define-cpointer-type _sqlhstmt)
(define-cpointer-type _sqlhdesc)

(define _sqllen (if win64? _int64 _long))
(define _sqlulen (if win64? _uint64 _ulong))

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

;; Docs at http://msdn.microsoft.com/en-us/library/ms712628%28v=VS.85%29.aspx
;; Notes on W functions: https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/unicode-function-arguments

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

(begin
  ;; Use W functions on Windows. On Unix and Mac OS, the base functions accept UTF-8.
  (define use-W? (eq? (system-type) 'windows))

  (define-syntax-parameter string->buf #f)
  (define-syntax-parameter make-buf #f)
  (define-syntax-parameter buf->string #f)
  (define-syntax-parameter buf-length #f)

  (define string->wbuf (case WCHAR-SIZE [(2) cpstr2] [(4) cpstr4]))
  (define (make-wbuf len-in-chars) (make-bytes (* len-in-chars WCHAR-SIZE)))
  (define (wbuf->string buf len-in-chars)
    (case WCHAR-SIZE
      [(2) (mkstr2 buf (* 2 len-in-chars))]
      [(4) (mkstr4 buf (* 4 len-in-chars))]))
  (define (wbuf-length buf) (quotient (bytes-length buf) WCHAR-SIZE))

  (define (string->1buf s) (string->bytes/utf-8 s))
  (define (make-1buf len-in-chars) (make-bytes len-in-chars))
  (define (1buf->string buf len-in-chars) (bytes->string/utf-8 buf #f 0 len-in-chars))
  (define (1buf-length buf) (bytes-length buf)))

;; (define-odbc+W name type) defines name as a function that calls either the
;; "normal" or "wide" version of the foreign function, depending on use-W?.
(define-syntax (define-odbc+W stx)
  (syntax-case stx ()
    [(_ name type)
     (with-syntax ([name1 (format-id #'name "~a1" #'name)]
                   [nameW (format-id #'name "~aW" #'name)])
       #'(begin (define-odbc name1
                  (syntax-parameterize ((string->buf (make-rename-transformer #'string->1buf))
                                        (make-buf (make-rename-transformer #'make-1buf))
                                        (buf->string (make-rename-transformer #'1buf->string))
                                        (buf-length (make-rename-transformer #'1buf-length)))
                    type)
                  #:c-id name)
                (define-odbc nameW
                  (syntax-parameterize ((string->buf (make-rename-transformer #'string->wbuf))
                                        (make-buf (make-rename-transformer #'make-wbuf))
                                        (buf->string (make-rename-transformer #'wbuf->string))
                                        (buf-length (make-rename-transformer #'wbuf-length)))
                    type))
                (define name (if use-W? nameW name1))))]))

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
        (value-buf : _intptr) ;; (the one case we care about takes int, not ptr)
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

(define-odbc+W SQLConnect
  (_fun (handle server user auth) ::
        (handle : _sqlhdbc)
        (server* : _bytes = (string->buf server))
        (_sqlsmallint = (buf-length server*))
        (user* : _bytes = (and user (string->buf user)))
        (_sqlsmallint = (if user* (buf-length user*) 0))
        (auth* : _bytes = (and auth (string->buf auth)))
        (_sqlsmallint = (if auth* (buf-length auth*) 0))
        -> _sqlreturn))

(define-odbc+W SQLDriverConnect
  (_fun (handle connection driver-completion) ::
        (handle : _sqlhdbc)
        (_pointer = #f)
        (connection* : _bytes = (and connection (string->buf connection)))
        (_sqlsmallint = (if connection* (buf-length connection*) 0))
        (_bytes = #f)
        (_sqlsmallint = 0)
        (out-length : (_ptr o _sqlsmallint))
        (driver-completion : _sqlusmallint)
        -> (status : _sqlreturn)
        -> status))

(define-odbc+W SQLDataSources
  (_fun (handle direction) ::
        (handle : _sqlhenv)
        (direction : _sqlusmallint)
        (server-buf : _bytes = (make-buf 1024))
        (_sqlsmallint = (buf-length server-buf))
        (server-length : (_ptr o _sqlsmallint))
        (descr-buf : _bytes = (make-buf 1024))
        (_sqlsmallint = (buf-length descr-buf))
        (descr-length : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status) (buf->string server-buf server-length))
                   (and (ok-status? status) (buf->string descr-buf descr-length)))))

(define-odbc+W SQLDrivers
  (_fun (handle direction) ::
        (handle : _sqlhenv)
        (direction : _sqlusmallint)
        (driver-buf : _bytes = (make-buf 1000))
        (_sqlsmallint = (buf-length driver-buf))
        (driver-length : (_ptr o _sqlsmallint))
        (attrs-buf : _bytes = (make-buf 2000))
        (_sqlsmallint = (buf-length attrs-buf))
        (attrs-length : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (if (ok-status? status)
               (values status
                       (buf->string driver-buf driver-length)
                       (buf->string attrs-buf attrs-length))
               (values status #f #f))))

(define-odbc+W SQLPrepare
  (_fun (handle stmt) ::
        (handle : _sqlhstmt)
        (stmt* : _bytes = (string->buf stmt))
        (_sqlinteger = (buf-length stmt*))
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

(define-odbc+W SQLDescribeCol
  (_fun (handle column column-buf) ::
        (handle : _sqlhstmt)
        (column : _sqlusmallint)
        (column-buf : _bytes)
        (_sqlsmallint = (buf-length column-buf))
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
                        (<= 0 column-len (buf-length column-buf))
                        (buf->string column-buf column-len))
                   data-type size digits nullable)))

(define-odbc SQLFetch
  (_fun _sqlhstmt
        -> _sqlreturn))

(define-odbc SQLGetData
  (_fun (handle column target-type buffer start) ::
        (handle : _sqlhstmt)
        (column : _sqlusmallint)
        (target-type : _sqlsmallint)
        (_gcpointer = (ptr-add buffer start))
        (_sqllen = (- (bytes-length buffer) start))
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

(define-odbc+W SQLGetDiagRec
  (_fun (handle-type handle rec-number) ::
        (handle-type : _sqlsmallint)
        (handle : _sqlhandle)
        (rec-number : _sqlsmallint)
        (sql-state-buf : _bytes = (make-buf 6))
        (native-errcode : (_ptr o _sqlinteger))
        (message-buf : _bytes = (make-buf 1024))
        (_sqlsmallint = (buf-length message-buf))
        (message-len : (_ptr o _sqlsmallint))
        -> (status : _sqlreturn)
        -> (values status
                   (and (ok-status? status)
                        (buf->string sql-state-buf 5))
                   native-errcode
                   (and (ok-status? status)
                        (buf->string message-buf message-len)))))

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

(define-odbc+W SQLTables
  (_fun (handle catalog schema table) ::
        (handle : _sqlhstmt)
        (catalog* : _bytes = (and catalog (string->buf catalog)))
        (_sqlsmallint = (if catalog* (buf-length catalog) 0))
        (schema* : _bytes = (and schema (string->buf schema)))
        (_sqlsmallint = (if schema* (buf-length schema) 0))
        (table* : _string = (and table (string->buf table)))
        (_sqlsmallint = (if table* (buf-length table) 0))
        (_bytes = #f)
        (_sqlsmallint = 0)
        -> _sqlreturn))
