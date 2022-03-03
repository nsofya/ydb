#pragma once

#if defined(__APPLE__) && defined(__arm64__)
#   include "pg_config-osx-arm64.h"
#elif defined(__APPLE__)
#   include "pg_config-osx.h"
#elif defined(_MSC_VER)
#   include "pg_config-win.h"
#else
#   include "pg_config-linux.h"
#endif
