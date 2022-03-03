# Generated by devtools/yamaker from nixpkgs 21.11.

LIBRARY()

OWNER(
    floatdrop
    g:cpp-contrib
)

VERSION(13.6)

ORIGINAL_SOURCE(mirror://postgresql/source/v13.6/postgresql-13.6.tar.bz2)

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause-Views AND
    BSD-3-Clause AND
    BSL-1.0 AND
    ISC AND
    Martin-Birgmeier AND
    PostgreSQL AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/libc_compat
    contrib/libs/openssl
)

ADDINCL(
    contrib/libs/libpq/src/backend
    GLOBAL contrib/libs/libpq/src/include
    contrib/libs/libpq/src/common
    contrib/libs/libpq/src/interfaces/libpq
    contrib/libs/libpq/src/port
)

NO_COMPILER_WARNINGS()

NO_RUNTIME()

CFLAGS(
    -DFRONTEND
    -DUNSAFE_STAT_OK
    -D_POSIX_PTHREAD_SEMANTICS
    -D_REENTRANT
    -D_THREAD_SAFE
)

SRCS(
    src/common/archive.c
    src/common/base64.c
    src/common/checksum_helper.c
    src/common/config_info.c
    src/common/controldata_utils.c
    src/common/d2s.c
    src/common/encnames.c
    src/common/exec.c
    src/common/f2s.c
    src/common/fe_memutils.c
    src/common/file_perm.c
    src/common/file_utils.c
    src/common/hashfn.c
    src/common/ip.c
    src/common/jsonapi.c
    src/common/keywords.c
    src/common/kwlookup.c
    src/common/link-canary.c
    src/common/logging.c
    src/common/md5.c
    src/common/pg_lzcompress.c
    src/common/pgfnames.c
    src/common/protocol_openssl.c
    src/common/psprintf.c
    src/common/relpath.c
    src/common/restricted_token.c
    src/common/rmtree.c
    src/common/saslprep.c
    src/common/scram-common.c
    src/common/sha2_openssl.c
    src/common/string.c
    src/common/stringinfo.c
    src/common/unicode_norm.c
    src/common/username.c
    src/common/wait_error.c
    src/common/wchar.c
    src/interfaces/libpq/fe-auth-scram.c
    src/interfaces/libpq/fe-auth.c
    src/interfaces/libpq/fe-connect.c
    src/interfaces/libpq/fe-exec.c
    src/interfaces/libpq/fe-lobj.c
    src/interfaces/libpq/fe-misc.c
    src/interfaces/libpq/fe-print.c
    src/interfaces/libpq/fe-protocol2.c
    src/interfaces/libpq/fe-protocol3.c
    src/interfaces/libpq/fe-secure-common.c
    src/interfaces/libpq/fe-secure-openssl.c
    src/interfaces/libpq/fe-secure.c
    src/interfaces/libpq/libpq-events.c
    src/interfaces/libpq/pqexpbuffer.c
    src/port/chklocale.c
    src/port/erand48.c
    src/port/fls.c
    src/port/getpeereid.c
    src/port/inet_net_ntop.c
    src/port/noblock.c
    src/port/path.c
    src/port/pg_bitutils.c
    src/port/pg_crc32c_sb8.c
    src/port/pg_strong_random.c
    src/port/pgcheckdir.c
    src/port/pgmkdirp.c
    src/port/pgsleep.c
    src/port/pgstrcasecmp.c
    src/port/pgstrsignal.c
    src/port/pqsignal.c
    src/port/qsort.c
    src/port/qsort_arg.c
    src/port/quotes.c
    src/port/snprintf.c
    src/port/sprompt.c
    src/port/strerror.c
    src/port/tar.c
    src/port/thread.c
)

IF (ARCH_X86_64)
    SRCS(
        src/port/pg_crc32c_sse42.c
        src/port/pg_crc32c_sse42_choose.c
    )
ENDIF()

IF (OS_WINDOWS)
    ADDINCL(
        contrib/libs/libpq/src/include/port
        contrib/libs/libpq/src/include/port/win32
        contrib/libs/libpq/src/include/port/win32_msvc
    )
    SRCS(
        src/interfaces/libpq/pthread-win32.c
        src/interfaces/libpq/win32.c
        src/port/getaddrinfo.c
        src/port/inet_aton.c
        src/port/open.c
        src/port/win32error.c
        src/port/win32setlocale.c
    )
ENDIF()

END()
