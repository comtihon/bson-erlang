% Use these macros to write/read numbers from bson or mongo binary format

-define (put_int32 (N), (N):32/signed-little).
-define (put_int64 (N), (N):64/signed-little).
-define (put_float (N), (N):64/float-little).

-define (get_int32 (N), N:32/signed-little).
-define (get_int64 (N), N:64/signed-little).
-define (get_float (N), N:64/float-little).
