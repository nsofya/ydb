OWNER(
    g:sqs
    g:kikimr
)

PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    sqs_requests_client.py
    sqs_matchers.py
    sqs_test_base.py
    test_account_actions.py
    test_acl.py
    test_counters.py
    test_garbage_collection.py
    test_generic_messaging.py
    test_fifo_messaging.py
    test_multinode_cluster.py
    test_multiplexing_tables_format.py
    test_ping.py
    test_polling.py
    test_queue_attributes_validation.py
    test_queues_managing.py
    test_quoting.py
    test_recompiles_requests.py
)

IF (SANITIZER_TYPE)
    TIMEOUT(2400) 
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        cpu:4
        ram:32
    )
ELSE()
    REQUIREMENTS(
        cpu:4
        ram:16
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydbd
    ydb/core/ymq/client/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/sqs
    contrib/python/xmltodict
    contrib/python/boto3
    contrib/python/botocore
)

FORK_SUBTESTS()

# SQS tests are not CPU or disk intensive,
# but they use sleeping for some events,
# so it would be secure to increase split factor.
# This increasing of split factor reduces test time
# to 15-20 seconds.
SPLIT_FACTOR(60)

END()
