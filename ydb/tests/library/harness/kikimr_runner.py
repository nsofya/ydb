# -*- coding: utf-8 -*-
import logging
import os
import shutil
import tempfile
import time
import itertools
from google.protobuf import text_format 

import ydb.tests.library.common.yatest_common as yatest_common

from ydb.tests.library.common.wait_for import wait_for
from . import daemon
from . import param_constants
from . import kikimr_config
from . import kikimr_node_interface
from . import kikimr_cluster_interface

import ydb.core.protos.blobstorage_config_pb2 as bs
from ydb.tests.library.predicates.blobstorage import blobstorage_controller_has_started_on_some_node
from library.python import resource


logger = logging.getLogger(__name__)


def get_unique_path_for_current_test(output_path, sub_folder):
    test_name = yatest_common.context.test_name or ""
    test_name = test_name.replace(':', '_')

    return os.path.join(output_path, test_name, sub_folder)


def ensure_path_exists(path):
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


def join(a, b):
    if a is None:
        a = ''
    if b is None:
        b = ''
    return os.path.join(a, b)


class KiKiMRNode(daemon.Daemon, kikimr_node_interface.NodeInterface):
    def __init__(self, node_idx, config_path, port_allocator, cluster_name, configurator,
                 udfs_dir=None, role='node', node_broker_port=None, tenant_affiliation=None, encryption_key=None):

        super(kikimr_node_interface.NodeInterface, self).__init__()
        self.node_id = node_idx
        self.__cwd = None
        self.__config_path = config_path
        self.__cluster_name = cluster_name
        self.__configurator = configurator
        self.__common_udfs_dir = udfs_dir

        self.__encryption_key = encryption_key
        self._tenant_affiliation = tenant_affiliation if tenant_affiliation is not None else 'dynamic'
        self.grpc_port = port_allocator.grpc_port
        self.mon_port = port_allocator.mon_port
        self.ic_port = port_allocator.ic_port
        self.grpc_ssl_port = port_allocator.grpc_ssl_port
        self.sqs_port = None
        if configurator.sqs_service_enabled:
            self.sqs_port = port_allocator.sqs_port

        self.__role = role 
        self.__node_broker_port = node_broker_port 
        self.__log_file = tempfile.NamedTemporaryFile(dir=self.cwd, prefix="logfile_", suffix=".log", delete=False)
        self.__cms_config_cache_file = tempfile.NamedTemporaryFile(
            dir=self.cwd,
            prefix="cms_config_cache_",
            delete=False
        )
        self.__cms_config_cache_file_name = self.__cms_config_cache_file.name
        daemon.Daemon.__init__(self, self.command, cwd=self.cwd, timeout=180, stderr_on_error_lines=240)

    @property
    def cwd(self):
        if self.__cwd is None:
            self.__cwd = ensure_path_exists(
                get_unique_path_for_current_test(
                    self.__configurator.output_path,
                    join(
                        self.__cluster_name, "{}_{}".format(
                            self.__role,
                            self.node_id
                        )
                    )
                )
            )
        return self.__cwd

    @property
    def cms_config_cache_file_name(self):
        return self.__cms_config_cache_file_name

    @property
    def command(self):
        return self.__make_run_command()

    def format_pdisk(self, pdisk_path, disk_size, **kwargs):
        logger.debug("Formatting pdisk %s on node %s, disk_size %s" % (pdisk_path, self, disk_size))
        if pdisk_path.startswith('SectorMap'):
            return

        with open(pdisk_path, "wb") as out:
            out.seek(disk_size - 1)
            out.write(b'\0')

    def __make_run_command(self):
        command = [self.__configurator.binary_path, "server"]
        if self.__common_udfs_dir is not None:
            command.append("--udfs-dir={}".format(self.__common_udfs_dir))

        if self.__configurator.suppress_version_check:
            command.append("--suppress-version-check")

        if self.__node_broker_port is not None:
            command.append("--node-broker=%s%s:%d" % (
                "grpcs://" if self.__configurator.grpc_ssl_enable else "",
                self.host,
                self.__node_broker_port))
        else: 
            command.append("--node=%d" % self.node_id)
 
        if self.__configurator.grpc_ssl_enable:
            command.append(
                "--ca=%s" % self.__configurator.grpc_tls_ca_path
            )

        if self.__role == 'slot':
            command.append(
                "--tenant=%s" % self._tenant_affiliation
            )
 
        if self.__configurator.grpc_ssl_enable:
            command.append(
                "--grpcs-port={}".format(
                    self.grpc_ssl_port
                )
            )

        command.extend(
            [
                "--yaml-config=%s" % join(self.__config_path, "config.yaml"),
                "--log-file-name=%s" % self.__log_file.name,
                "--grpc-port=%s" % self.grpc_port,
                "--mon-port=%d" % self.mon_port,
                "--ic-port=%d" % self.ic_port,
                "--cms-config-cache-file=%s" % self.cms_config_cache_file_name,
            ]
        )

        if self.__encryption_key is not None:
            command.extend(["--key-file", self.__encryption_key])

        if self.sqs_port is not None:
            command.extend(["--sqs-port=%d" % self.sqs_port])

        logger.info('CFG_DIR_PATH="%s"', self.__config_path)
        logger.info("Final command: %s", ' '.join(command).replace(self.__config_path, '$CFG_DIR_PATH'))
        return command

    def stop(self):
        try:
            super(KiKiMRNode, self).stop()
        finally:
            logger.info("Stopped node %s", self)

    def kill(self):
        try:
            super(KiKiMRNode, self).kill()
            self.start()
        finally:
            logger.info("Killed node %s", self)

    def send_signal(self, signal):
        self.daemon.process.send_signal(signal)

    @property
    def host(self):
        return 'localhost'

    @property
    def hostname(self):
        return kikimr_config.get_fqdn()

    @property
    def port(self):
        return self.grpc_port

    @property
    def pid(self):
        return self.daemon.process.pid

    def start(self):
        try:
            super(KiKiMRNode, self).start()
        finally:
            logger.info("Started node %s", self)


class KiKiMR(kikimr_cluster_interface.KiKiMRClusterInterface):
    def __init__(self, configurator=None, cluster_name=''):
        super(KiKiMR, self).__init__()

        self.__tmpdir = tempfile.mkdtemp(prefix="kikimr_" + cluster_name + "_")
        self.__common_udfs_dir = None
        self.__cluster_name = cluster_name
        self.__configurator = kikimr_config.KikimrConfigGenerator() if configurator is None else configurator
        self.__port_allocator = self.__configurator.port_allocator
        self._nodes = {}
        self._slots = {}
        self.__server = 'localhost'
        self.__client = None
        self.__storage_pool_id_allocator = itertools.count(1)
        self.__config_path = None
        self._slot_index_allocator = itertools.count(1)
        self._node_index_allocator = itertools.count(1)
        self.default_channel_bindings = None

    @property
    def config(self):
        return self.__configurator

    @property
    def nodes(self):
        return self._nodes

    @property
    def slots(self):
        return self._slots

    @property
    def domain_name(self):
        return self.__configurator.domain_name

    @property
    def server(self):
        return self.__server

    def __call_kikimr_new_cli(self, cmd, connect_to_server=True):
        server = 'grpc://{server}:{port}'.format(server=self.server, port=self.nodes[1].port)
        full_command = [self.__configurator.binary_path]
        if connect_to_server:
            full_command += ["--server={server}".format(server=server)]
        full_command += cmd

        logger.debug("Executing command = {}".format(full_command))
        try:
            return yatest_common.execute(full_command)
        except yatest_common.ExecutionError as e:
            logger.exception("KiKiMR command '{cmd}' failed with error: {e}\n\tstdout: {out}\n\tstderr: {err}".format(
                cmd=" ".join(str(x) for x in full_command),
                e=str(e),
                out=e.execution_result.std_out,
                err=e.execution_result.std_err
            ))
            raise

    def start(self):
        """
        Safely starts kikimr instance.
        Do not override this method.
        """
        try:
            logger.debug("Working directory: " + self.__tmpdir)
            self.__run()
            return self

        except Exception:
            logger.exception("KiKiMR start failed")
            self.stop()
            raise

    def __run(self):
        self.__client = None

        self.__instantiate_udfs_dir()
        self.__write_configs()

        for _ in self.__configurator.all_node_ids():
            self.__register_node()

        for node_id in self.__configurator.all_node_ids():
            self.__run_node(node_id)

        self.__wait_for_bs_controller_to_start()
        self.__add_bs_box()
 
        pools = {}

        for p in self.__configurator.dynamic_storage_pools:
            self.add_storage_pool(
                name=p['name'],
                kind=p['kind'],
                pdisk_user_kind=p['pdisk_user_kind'],
            )
            pools[p['name']] = p['kind']

        self.client.bind_storage_pools(self.domain_name, pools)
        default_pool_name = list(pools.keys())[0]
        self.default_channel_bindings = {idx: default_pool_name for idx in range(3)}
        logger.info("Cluster started and initialized")

        self.client.add_config_item(
            resource.find(
                "harness/resources/default_profile.txt"
            )
        )

    def __run_node(self, node_id):
        """
        :returns started KiKiMRNode instance
        Can be overriden.
        """
        self.__format_disks(node_id)
        self._nodes[node_id].start()
        return self._nodes[node_id]

    def __register_node(self):
        node_index = next(self._node_index_allocator)
        self._nodes[node_index] = KiKiMRNode(
            node_index,
            self.config_path,
            port_allocator=self.__port_allocator.get_node_port_allocator(node_index),
            cluster_name=self.__cluster_name,
            configurator=self.__configurator,
            udfs_dir=self.__common_udfs_dir,
        )
        return self._nodes[node_index]

    def register_slots(self, database, count=1, encryption_key=None):
        return [self.register_slot(database, encryption_key) for _ in range(count)]

    def register_and_start_slots(self, database, count=1, encryption_key=None):
        slots = self.register_slots(database, count, encryption_key)
        for slot in slots:
            slot.start()
        return slots

    def register_slot(self, tenant_affiliation=None, encryption_key=None):
        return self._register_slot(tenant_affiliation, encryption_key)

    def _register_slot(self, tenant_affiliation=None, encryption_key=None):
        slot_index = next(self._slot_index_allocator)
        node_broker_port = (
            self.nodes[1].grpc_ssl_port if self.__configurator.grpc_ssl_enable
            else self.nodes[1].grpc_port
        )
        self._slots[slot_index] = KiKiMRNode(
            slot_index,
            self.config_path, 
            port_allocator=self.__port_allocator.get_slot_port_allocator(slot_index),
            cluster_name=self.__cluster_name, 
            configurator=self.__configurator, 
            udfs_dir=self.__common_udfs_dir, 
            role='slot', 
            node_broker_port=node_broker_port,
            tenant_affiliation=tenant_affiliation,
            encryption_key=encryption_key,
        ) 
        return self._slots[slot_index]
 
    def __stop_node(self, node): 
        ret = None 
        try: 
            node.stop() 
        except daemon.DaemonError as exceptions:
            ret = exceptions 
        else: 
            if self.__tmpdir is not None: 
                shutil.rmtree(self.__tmpdir, ignore_errors=True) 
            if self.__common_udfs_dir is not None: 
                shutil.rmtree(self.__common_udfs_dir, ignore_errors=True) 
        return ret 
 
    def stop(self):
        saved_exceptions = []

        for slot in self.slots.values(): 
            exception = self.__stop_node(slot) 
            if exception is not None: 
                saved_exceptions.append(exception) 
 
        for node in self.nodes.values():
            exception = self.__stop_node(node)
            if exception is not None:
                saved_exceptions.append(exception)

        self.__port_allocator.release_ports()
 
        if saved_exceptions:
            raise daemon.SeveralDaemonErrors(saved_exceptions)

    @property
    def config_path(self):
        if self.__config_path is None:
            self.__config_path = ensure_path_exists(
                get_unique_path_for_current_test(
                    self.__configurator.output_path,
                    join(
                        self.__cluster_name, "kikimr_configs"
                    )
                )
            )
        return self.__config_path

    def __write_configs(self):
        self.__configurator.write_proto_configs(self.config_path)

    def __instantiate_udfs_dir(self):
        to_load = self.__configurator.get_yql_udfs_to_load()
        if len(to_load) == 0:
            return
        self.__common_udfs_dir = tempfile.mkdtemp(prefix="common_udfs")
        for udf_path in to_load:
            link_name = os.path.join(self.__common_udfs_dir, os.path.basename(udf_path))
            os.symlink(udf_path, link_name)
        return self.__common_udfs_dir

    def __format_disks(self, node_id):
        for pdisk in self.__configurator.pdisks_info:
            if pdisk['node_id'] != node_id:
                continue

            self.nodes[node_id].format_pdisk(**pdisk)

    def __add_bs_box(self): 
        request = bs.TConfigRequest()
 
        for node_id in self.__configurator.all_node_ids(): 
            cmd = request.Command.add()
            cmd.DefineHostConfig.HostConfigId = node_id
            for drive in self.__configurator.pdisks_info:
                if drive['node_id'] != node_id:
                    continue

                drive_proto = cmd.DefineHostConfig.Drive.add() 
                drive_proto.Path = drive['pdisk_path']
                drive_proto.Kind = drive['pdisk_user_kind']
                drive_proto.Type = drive.get('pdisk_type', 0)
 
        cmd = request.Command.add() 
        cmd.DefineBox.BoxId = 1 
        for node_id, node in self.nodes.items(): 
            host = cmd.DefineBox.Host.add() 
            host.Key.Fqdn = node.host 
            host.Key.IcPort = node.ic_port 
            host.HostConfigId = node_id 
 
        self._bs_config_invoke(request)
 
    def _bs_config_invoke(self, request):
        timeout = yatest_common.plain_or_under_sanitizer(120, 240)
        sleep = 5
        retries, success = timeout / sleep, False
        while retries > 0 and not success:
            try:
                self.__call_kikimr_new_cli(
                    [
                        "admin",
                        "blobstorage",
                        "config",
                        "invoke",
                        "--proto=%s" % text_format.MessageToString(request)
                    ]
                )
                success = True

            except Exception as e:
                logger.error("Failed to execute, %s", str(e))
                retries -= 1
                time.sleep(sleep)

                if retries == 0:
                    raise

    def add_storage_pool(self, name=None, kind="rot", pdisk_user_kind=0, erasure=None):
        if erasure is None:
            erasure = self.__configurator.static_erasure
        request = bs.TConfigRequest()
        cmd = request.Command.add() 
        cmd.DefineStoragePool.BoxId = 1 

        pool_id = cmd.DefineStoragePool.StoragePoolId = next(self.__storage_pool_id_allocator)
        if name is None:
            name = "dynamic_storage_pool:%s" % pool_id
        cmd.DefineStoragePool.StoragePoolId = pool_id
        cmd.DefineStoragePool.Name = name
        cmd.DefineStoragePool.Kind = kind
        cmd.DefineStoragePool.ErasureSpecies = str(erasure)
        cmd.DefineStoragePool.VDiskKind = "Default" 
        cmd.DefineStoragePool.NumGroups = 2

        pdisk_filter = cmd.DefineStoragePool.PDiskFilter.add()
        pdisk_filter.Property.add().Type = 0
        pdisk_filter.Property.add().Kind = pdisk_user_kind
        self._bs_config_invoke(request)
        return name
 
    def __wait_for_bs_controller_to_start(self):
        monitors = [node.monitor for node in self.nodes.values()]

        def predicate():
            return blobstorage_controller_has_started_on_some_node(monitors)

        timeout_seconds = yatest_common.plain_or_under_sanitizer(120, 240)
        bs_controller_started = wait_for(
            predicate=predicate, timeout_seconds=timeout_seconds, step_seconds=1.0, multiply=1.3
        )
        assert bs_controller_started


class KikimrExternalNode(daemon.ExternalNodeDaemon, kikimr_node_interface.NodeInterface):
    def __init__(
            self, node_id, host, port, mon_port, ic_port, mbus_port, configurator=None, slot_id=None):
        super(KikimrExternalNode, self).__init__(host)
        self.__node_id = node_id
        self.__host = host
        self.__port = port
        self.__grpc_port = port
        self.__mon_port = mon_port
        self.__ic_port = ic_port
        self.__configurator = configurator
        self.__mbus_port = mbus_port
        self.logger = logger.getChild(self.__class__.__name__)
        if slot_id is not None:
            self.__slot_id = "%s" % str(self.__ic_port)
        else:
            self.__slot_id = None

        self._can_update = None
        self.current_version_idx = 0
        self.versions = [
            param_constants.kikimr_last_version_deploy_path,
            param_constants.kikimr_next_version_deploy_path,
        ]

    @property
    def can_update(self):
        if self._can_update is None:
            choices = self.ssh_command('ls %s*' % param_constants.kikimr_binary_deploy_path, raise_on_error=True)
            choices = choices.split()
            self.logger.error("Current available choices are: %s" % choices)
            self._can_update = True
            for version in self.versions:
                if version not in choices:
                    self._can_update &= False
        return self._can_update

    def start(self):
        if self.__slot_id is None:
            return self.ssh_command("sudo start kikimr")
        return self.ssh_command(
            [
                "sudo", "start",
                "kikimr-multi",
                "slot={}".format(self.__slot_id),
                "tenant=dynamic",
                "mbus={}".format(self.__mbus_port),
                "grpc={}".format(self.__grpc_port),
                "mon={}".format(self.__mon_port),
                "ic={}".format(self.__ic_port),
            ]
        )

    def stop(self):
        if self.__slot_id is None:
            return self.ssh_command("sudo stop kikimr")
        return self.ssh_command(
            [
                "sudo", "stop",
                "kikimr-multi",
                "slot={}".format(self.__slot_id),
                "tenant=dynamic",
                "mbus={}".format(self.__mbus_port),
                "grpc={}".format(self.__grpc_port),
                "mon={}".format(self.__mon_port),
                "ic={}".format(self.__ic_port),
            ]
        )

    @property
    def cwd(self):
        assert False, "not supported"

    @property
    def mon_port(self):
        return self.__mon_port

    @property
    def pid(self):
        return None

    def is_alive(self):
        # TODO implement check
        return True

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def grpc_ssl_port(self):
        # TODO(gvit): support in clusters
        return None

    @property
    def grpc_port(self):
        return self.__port

    @property
    def mbus_port(self):
        return self.__mbus_port

    @property
    def ic_port(self):
        return self.__ic_port

    @property
    def node_id(self):
        return self.__node_id

    @property
    def logs_directory(self):
        folder = 'kikimr_%s' % self.__slot_id if self.__slot_id else 'kikimr'
        return "/Berkanavt/{}/logs".format(folder)

    def update_binary_links(self):
        self.ssh_command("sudo rm -rf %s" % param_constants.kikimr_binary_deploy_path)
        self.ssh_command(
            "sudo cp -l %s %s" % (
                self.versions[self.current_version_idx],
                param_constants.kikimr_binary_deploy_path,
            )
        )

    def switch_version(self):
        if not self.can_update:
            self.logger.info("Next version is not available. Cannot change versions.")
            return None

        self.current_version_idx ^= 1
        self.update_binary_links()

    def prepare_artifacts(self, cluster_yml):
        self.copy_file_or_dir(
            param_constants.kikimr_configure_binary_path(), param_constants.kikimr_configure_binary_deploy_path)
        local_drivers_path = (param_constants.kikimr_driver_path(), param_constants.next_version_kikimr_driver_path())

        for version, local_driver in zip(self.versions, local_drivers_path):
            self.ssh_command("sudo rm -rf %s" % version)
            if local_driver is not None:
                self.copy_file_or_dir(
                    local_driver, version)
                self.ssh_command("sudo /sbin/setcap 'CAP_SYS_RAWIO,CAP_SYS_NICE=ep' %s" % version)

        self.update_binary_links()
        self.ssh_command("sudo mkdir -p %s" % param_constants.kikimr_configuration_deploy_path)
        self.copy_file_or_dir(cluster_yml, param_constants.kikimr_cluster_yaml_deploy_path)
        self.ssh_command(param_constants.generate_configs_cmd())
        self.ssh_command(
            param_constants.generate_configs_cmd(
                "--dynamic"
            )
        )

    def format_pdisk(self, pdisk_id):
        pass

    def cleanup_disk(self, path):
        self.ssh_command(
            'sudo dd if=/dev/zero of={} bs=1M count=1 status=none;'.format(path),
            raise_on_error=True)

    def cleanup_disks(self):
        self.ssh_command(
            "for X in /dev/disk/by-partlabel/kikimr_*; "
            "do sudo dd if=/dev/zero of=$X bs=1M count=1 status=none; done",
            raise_on_error=True)
