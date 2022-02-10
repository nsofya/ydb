LIBRARY() 
 
OWNER( 
    ienkovich 
    g:kikimr 
) 
 
SRCS( 
    config_helpers.cpp 
    config_helpers.h 
    config_index.cpp 
    config_index.h 
    configs_cache.cpp
    configs_cache.h
    configs_config.cpp 
    configs_config.h 
    configs_dispatcher.cpp 
    configs_dispatcher.h 
    console.cpp 
    console.h 
    console_configs_manager.cpp 
    console_configs_manager.h 
    console_configs_provider.cpp 
    console_configs_provider.h 
    console_configs_subscriber.cpp
    console_configs_subscriber.h
    console_impl.h 
    console_tenants_manager.cpp 
    console_tenants_manager.h 
    console__add_config_subscription.cpp 
    console__alter_tenant.cpp 
    console__cleanup_subscriptions.cpp 
    console__configure.cpp 
    console__create_tenant.cpp 
    console__init_scheme.cpp 
    console__load_state.cpp 
    console__remove_computational_units.cpp 
    console__remove_config_subscription.cpp 
    console__remove_config_subscriptions.cpp 
    console__remove_tenant.cpp 
    console__remove_tenant_done.cpp 
    console__remove_tenant_failed.cpp 
    console__replace_config_subscriptions.cpp 
    console__revert_pool_state.cpp
    console__scheme.h 
    console__set_config.cpp 
    console__toggle_config_validator.cpp 
    console__update_confirmed_subdomain.cpp 
    console__update_last_provided_config.cpp 
    console__update_pool_state.cpp 
    console__update_subdomain_key.cpp 
    console__update_tenant_state.cpp 
    defs.h 
    http.cpp 
    http.h 
    immediate_controls_configurator.cpp 
    immediate_controls_configurator.h 
    log_settings_configurator.cpp 
    log_settings_configurator.h 
    modifications_validator.cpp 
    modifications_validator.h 
    net_classifier_updater.cpp
    shared_cache_configurator.cpp
    shared_cache_configurator.h
    tx_processor.cpp 
    tx_processor.h 
    util.cpp
    util.h
) 
 
PEERDIR( 
    library/cpp/actors/core
    library/cpp/actors/http
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/blobstorage
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/cms/console/validators
    ydb/core/control
    ydb/core/engine/minikql
    ydb/core/mind
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/util
    ydb/library/aclib
    ydb/public/api/protos
    ydb/public/lib/operation_id
) 
 
END() 

RECURSE(
    validators
)

RECURSE_FOR_TESTS(
    ut
)
