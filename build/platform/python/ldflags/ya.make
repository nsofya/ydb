RESOURCES_LIBRARY()

SUBSCRIBER(g:contrib)

IF (USE_SYSTEM_PYTHON)
    IF (OS_LINUX)
        LDFLAGS("-L$EXTERNAL_PYTHON_RESOURCE_GLOBAL/python/lib/x86_64-linux-gnu -lpython${PY_VERSION}")
    ELSEIF (OS_DARWIN)
        LDFLAGS("-L$EXTERNAL_PYTHON_RESOURCE_GLOBAL/python/Python.framework/Versions/${PY_FRAMEWORK_VERSION}/lib -lpython${PY_VERSION}")
    ELSEIF (OS_WINDOWS)
        LDFLAGS("/LIBPATH:$EXTERNAL_PYTHON_RESOURCE_GLOBAL/python/libs")
    ENDIF()
ELSEIF (NOT USE_ARCADIA_PYTHON)
    LDFLAGS($PYTHON_LDFLAGS $PYTHON_LIBRARIES)
ENDIF()

END()
