PY23_LIBRARY()

SUBSCRIBER(
    g:yt
)

PEERDIR(
    yt/python/yt/type_info
)

PY_SRCS(
    NAMESPACE yandex.type_info

    __init__.py
    typing.py
    type_base.py
)

END()
