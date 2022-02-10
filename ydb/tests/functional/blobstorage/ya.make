OWNER(g:kikimr)
 
PY3TEST()
 
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    pdisk_format_info.py
    replication.py
    self_heal.py
    tablet_channel_migration.py
)
 
IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:4
        ram:16
    )
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    REQUIREMENTS(
        cpu:4
        ram:16
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()
 
SPLIT_FACTOR(20)

DEPENDS(
    ydb/apps/ydbd
)
 
PEERDIR(
    ydb/tests/library
    contrib/python/PyHamcrest
)
 
FORK_SUBTESTS()
FORK_TEST_FILES()
 
END()
