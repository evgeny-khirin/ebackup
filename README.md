# Backup System written in Erlang

Supports versioning backup with deduplication data.

Contains 64-bits storage engine for Erlang with full ACID supprt.

# Building

* OS supported: Linux 32 and 64 bits.

* Since it does not have make file, you'll need Erlang to build the compiler. There is
  build.erl file in root directory. It is my replacement of make, written in Erlang.
  You need to start Erlang in the root directory:
  
      evgeny@wheezy:~/work/ebackup$ erl
      Erlang/OTP 17 [erts-6.2] [source] [async-threads:10] [kernel-poll:false]

      Eshell V6.2  (abort with ^G)
      1> c("build.erl").
      {ok,build}
      2> build:compile().
