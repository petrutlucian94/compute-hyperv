# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Management class for Cluster VM operations."""

import platform

from nova.compute import power_state
from nova.compute import task_states
from nova.compute import vm_states
from nova.virt import driver
from nova.volume import cinder
import nova.conf
from nova import context
from nova import network
from nova import objects
from nova.virt import block_device
from os_win import exceptions as os_win_exc
from os_win import utilsfactory
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall

from hyperv.i18n import _LI, _LE
from hyperv.nova import serialconsoleops
from hyperv.nova import vmops

LOG = logging.getLogger(__name__)

hyperv_cluster_opts = [
    cfg.IntOpt('cluster_event_check_interval',
               default=2),
]

CONF = nova.conf.CONF
CONF.register_opts(hyperv_cluster_opts, 'hyperv')


class ClusterOps(object):

    def __init__(self, compute_driver):
        self._compute_driver = compute_driver
        self._volume_api = cinder.API()

        self._clustutils = utilsfactory.get_clusterutils()
        self._vmutils = utilsfactory.get_vmutils()
        self._pathutils = utilsfactory.get_pathutils()
        self._migrationutils = utilsfactory.get_migrationutils()

        self._clustutils.check_cluster_state()
        self._instance_map = {}

        self._daemon = None
        self._this_node = self._clustutils.get_node_name()

        self._context = context.get_admin_context()
        self._network_api = network.API()
        self._vmops = vmops.VMOps()
        self._serial_console_ops = serialconsoleops.SerialConsoleOps()

    def get_instance_host(self, instance):
        return self._clustutils.get_vm_host(instance.name)

    def add_to_cluster(self, instance):
        try:
            self._clustutils.add_vm_to_cluster(instance.name)
            self._instance_map[instance.name] = instance.uuid
        except os_win_exc.HyperVClusterException:
            LOG.exception(_LE('Adding instance to cluster failed.'),
                          instance=instance)

    def remove_from_cluster(self, instance):
        try:
            if self._clustutils.vm_exists(instance.name):
                self._clustutils.delete(instance.name)
            self._instance_map.pop(instance.name, None)
        except os_win_exc.HyperVClusterException:
            LOG.exception(_LE('Removing instance from cluster failed.'),
                          instance=instance)

    def post_migration(self, instance):
        # update instance cache
        self._instance_map[instance.name] = instance.uuid

    def start_failover_listener_daemon(self):
        """Start the daemon failover listener."""

        def _looper():
            try:
                self._clustutils.monitor_vm_failover(self._failover_migrate)
            except Exception:
                LOG.exception(_LE('Exception occured during failover '
                                  'observation / migration.'))

        self._daemon = loopingcall.FixedIntervalLoopingCall(_looper)

        self._daemon.start(
            interval=CONF.hyperv.cluster_event_check_interval)

    def _failover_migrate(self, instance_name, old_host, new_host):
        """This method will check if the generated event is a legitimate
        failover to this node. If it is, it will proceed to prepare the
        failovered VM if necessary and update the owner of the compute vm in
        nova and ports in neutron.
        """
        LOG.info(_LI('Checking instance failover %(instance)s to %(new_host)s '
                     'from host %(old_host)s.'),
                 {'instance': instance_name,
                  'new_host': new_host,
                  'old_host': old_host})

        instance = self._get_instance_by_name(instance_name)
        nw_info = self._network_api.get_instance_nw_info(self._context,
                                                         instance)

        if not instance:
            # Some instances on the hypervisor may not be tracked by nova
            LOG.debug('Instance %s does not exist in nova. Skipping.',
                      instance_name)
            return

        if instance.task_state == task_states.MIGRATING:
            # In case of live migration triggered by the user, we get the
            # event that the instance changed host but we do not want
            # to treat it as a failover.
            LOG.debug('Instance %s is live migrating.', instance_name)
            return
        if instance.host.upper() == self._this_node.upper():
            return

        if old_host and old_host.upper() == self._this_node.upper():
            LOG.debug('Actions at source node.')
            self._vmops.unplug_vifs(instance, nw_info)
            return
        elif new_host.upper() != self._this_node.upper():
            LOG.debug('Instance %s did not failover to this node.',
                      instance_name)
            return

        LOG.info(_LI('Instance %(instance)s  failover to %(host)s.'),
                 {'instance': instance_name,
                  'host': new_host})

        self._failover_instance(instance, old_host, nw_info)

    def _failover_migrate_networks(self, instance, source):
        """This is called after a VM failovered to this node.
        This will change the owner of the neutron ports to this node.
        """
        migration = {'source_compute': source,
                     'dest_compute': self._this_node, }

        self._network_api.setup_networks_on_host(
            self._context, instance, self._this_node)
        self._network_api.migrate_instance_start(
            self._context, instance, migration)
        self._network_api.setup_networks_on_host(
            self._context, instance, self._this_node)
        self._network_api.migrate_instance_finish(
            self._context, instance, migration)
        self._network_api.setup_networks_on_host(
            self._context, instance, self._this_node)
        self._network_api.setup_networks_on_host(
            self._context, instance, source, teardown=True)

    def _get_instance_by_name(self, instance_name):
        # Since from a failover we only get the instance name
        # we need to find it's uuid so we can retrieve the instance
        # object from nova. We keep a map from the instance name to the
        # instance uuid. First we try to get the vm uuid from that map
        # if it's not there, we try to get it from the instance notes,
        # this may fail (during a failover for example, the vm will not
        # be at the source node anymore) and lastly we try and get the
        # vm uuid from the database.
        vm_uuid = self._instance_map.get(instance_name)
        if not vm_uuid:
            try:
                vm_uuid = self._vmutils.get_instance_uuid(instance_name)
                self._instance_map[instance_name] = vm_uuid
            except os_win_exc.HyperVVMNotFoundException:
                pass

        if not vm_uuid:
            self._update_instance_map()
            vm_uuid = self._instance_map.get(instance_name)

        if not vm_uuid:
            LOG.debug("Instance %s cannot be found in Nova.", instance_name)
            return

        return objects.Instance.get_by_uuid(self._context, vm_uuid)

    def _update_instance_map(self):
        expected_attrs = ['id', 'uuid', 'name']

        for server in objects.InstanceList.get_by_filters(
                self._context, {'deleted': False},
                expected_attrs=expected_attrs):
            self._instance_map[server.name] = server.uuid

    def _get_instance_block_device_info(self, instance,
                                        refresh_conn_info=False):
        """Transform block devices to the driver block_device format."""
        bdms = objects.BlockDeviceMappingList.get_by_instance_uuid(
            self._context, instance.uuid)
        block_device_info = driver.get_block_device_info(instance, bdms)

        if not refresh_conn_info:
            # if the block_device_mapping has no value in connection_info
            # (returned as None), don't include in the mapping
            block_device_info['block_device_mapping'] = [
                bdm for bdm in driver.block_device_info_get_mapping(
                                    block_device_info)
                if bdm.get('connection_info')]
        else:
            block_device.refresh_conn_infos(
                driver.block_device_info_get_mapping(block_device_info),
                self._context, instance,
                self._volume_api,
                self._compute_driver)

    def _evacuate_instance(self, instance):
        raise NotImplementedError()

    def _failover_instance(self, instance, old_host, network_info):
        # TODO: retrieve the expected new state based on the resource state.
        # Based on that, we'll have to update the Nova instance status, as well
        # as changing the vm power state, if needed.
        # TODO: cleanup in case of errors (not sure if the cluster service will
        # request a resource 'Terminate' in this case)

        # if instance.vm_state == vm_states.ERROR:
        #     # Sometimes during a failover nova can set the instance state
        #     # to error depending on how much time the failover takes.
        #     instance.vm_state = vm_states.ACTIVE
        # if instance.power_state == power_state.NOSTATE:
        #     instance.power_state = power_state.RUNNING

        # This will also request Cinder to initiate volume connection.
        # TODO: maybe disconnect the volumes from the old host?
        # (call cinder to remove exports)
        block_device_info = self._get_instance_block_device_info(
            instance, refresh_conn_info=True)
        self._volumeops.connect_volumes(block_device_info)

        self._import_instance(instance)
        self._volumeops.fix_instance_volume_disk_paths(
            instance.name, block_device_info, is_planned_vm=True)
        self._migrationutils.realize_vm(instance.name)

        self._failover_migrate_networks(instance, old_host)

        # platform.node is used so that this remains consistent
        # to what we report to Nova.
        instance.host = platform.node()
        instance.node = platform.node()
        instance.save(expected_task_state=[None])

        if new_power_state == power_state.RUNNING:
            self._vmops.power_on(instance, network_info=network_info)

    def _import_instance(self, instance):
        snapshot_dir = self._pathutils.get_instance_snapshot_dir(instance.name)
        config_file_path = self._pathutils.get_vm_config_file_path(
            instance.name, use_export_dir=False)
        self._migrationutils.import_vm_definition(config_file_path,
                                                  snapshot_dir)
