# Copyright 2013 Cloudbase Solutions Srl
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

"""
Management class for migration / resize operations.
"""
import os

from nova import exception
from nova.virt import configdrive
from os_win import utilsfactory
from oslo_log import log as logging
from oslo_utils import excutils
from oslo_utils import units

from hyperv.i18n import _, _LW
from hyperv.nova import block_device_manager
from hyperv.nova import constants
from hyperv.nova import imagecache
from hyperv.nova import pathutils
from hyperv.nova import vmops
from hyperv.nova import volumeops

LOG = logging.getLogger(__name__)


class MigrationOps(object):
    def __init__(self):
        self._vmutils = utilsfactory.get_vmutils()
        self._vhdutils = utilsfactory.get_vhdutils()
        self._pathutils = pathutils.PathUtils()
        self._volumeops = volumeops.VolumeOps()
        self._vmops = vmops.VMOps()
        self._imagecache = imagecache.ImageCache()
        self._block_dev_man = block_device_manager.BlockDeviceInfoManager()

    def _move_vm_files(self, instance):
        instance_path = self._pathutils.get_instance_dir(instance.name)
        revert_path = self._pathutils.get_instance_migr_revert_dir(
            instance.name, remove_dir=True, create_dir=True)

        # copy the given instance's files to a _revert folder, as backup.
        LOG.debug("Moving instance files to a revert path: %s",
                  revert_path, instance=instance)
        self._pathutils.move_folder_files(instance_path, revert_path)

        return revert_path

    def _check_target_flavor(self, instance, flavor):
        new_root_gb = flavor.root_gb
        curr_root_gb = instance.flavor.root_gb

        if new_root_gb < curr_root_gb:
            raise exception.InstanceFaultRollback(
                exception.CannotResizeDisk(
                    reason=_("Cannot resize the root disk to a smaller size. "
                             "Current size: %(curr_root_gb)s GB. Requested "
                             "size: %(new_root_gb)s GB.") % {
                                 'curr_root_gb': curr_root_gb,
                                 'new_root_gb': new_root_gb}))

    def migrate_disk_and_power_off(self, context, instance, dest,
                                   flavor, network_info,
                                   block_device_info=None, timeout=0,
                                   retry_interval=0):
        LOG.debug("migrate_disk_and_power_off called", instance=instance)

        self._check_target_flavor(instance, flavor)

        self._vmops.power_off(instance, timeout, retry_interval)
        instance_path = self._move_vm_files(instance)

        self._vmops.destroy(instance, destroy_disks=True)

        # return the instance's path location.
        return instance_path

    def confirm_migration(self, context, migration, instance, network_info):
        LOG.debug("confirm_migration called", instance=instance)

        self._pathutils.get_instance_migr_revert_dir(instance.name,
                                                     remove_dir=True)

    def _revert_migration_files(self, instance_name):
        instance_path = self._pathutils.get_instance_dir(
            instance_name, create_dir=False, remove_dir=True)

        revert_path = self._pathutils.get_instance_migr_revert_dir(
            instance_name)
        self._pathutils.rename(revert_path, instance_path)

    def _check_and_attach_config_drive(self, instance, vm_gen):
        if configdrive.required_by(instance):
            configdrive_path = self._pathutils.lookup_configdrive_path(
                instance.name)
            if configdrive_path:
                self._vmops.attach_config_drive(instance, configdrive_path,
                                                vm_gen)
            else:
                raise exception.ConfigDriveNotFound(
                    instance_uuid=instance.uuid)

    def finish_revert_migration(self, context, instance, network_info,
                                block_device_info=None, power_on=True):
        LOG.debug("finish_revert_migration called", instance=instance)
        self._revert_migration_files(instance.name)

        image_meta = self._imagecache.get_image_details(context, instance)
        vm_gen = self._vmops.get_image_vm_generation(instance.uuid, image_meta)
        self._check_and_update_disks(context, instance, vm_gen, image_meta,
                                     block_device_info)

        self._vmops.create_instance(context, instance, network_info,
                                    block_device_info, vm_gen, image_meta)

        self._check_and_attach_config_drive(instance, vm_gen)
        self._vmops.set_boot_order(instance.name, vm_gen, block_device_info)
        if power_on:
            self._vmops.power_on(instance, network_info=network_info)

    def _merge_base_vhd(self, diff_vhd_path, base_vhd_path):
        base_vhd_copy_path = os.path.join(os.path.dirname(diff_vhd_path),
                                          os.path.basename(base_vhd_path))
        try:
            LOG.debug('Copying base disk %(base_vhd_path)s to '
                      '%(base_vhd_copy_path)s',
                      {'base_vhd_path': base_vhd_path,
                       'base_vhd_copy_path': base_vhd_copy_path})
            self._pathutils.copyfile(base_vhd_path, base_vhd_copy_path)

            LOG.debug("Reconnecting copied base VHD "
                      "%(base_vhd_copy_path)s and diff "
                      "VHD %(diff_vhd_path)s",
                      {'base_vhd_copy_path': base_vhd_copy_path,
                       'diff_vhd_path': diff_vhd_path})
            self._vhdutils.reconnect_parent_vhd(diff_vhd_path,
                                                base_vhd_copy_path)

            LOG.debug("Merging differential disk %s into its parent.",
                      diff_vhd_path)
            self._vhdutils.merge_vhd(diff_vhd_path)

            # Replace the differential VHD with the merged one
            self._pathutils.rename(base_vhd_copy_path, diff_vhd_path)
        except Exception:
            with excutils.save_and_reraise_exception():
                if self._pathutils.exists(base_vhd_copy_path):
                    self._pathutils.remove(base_vhd_copy_path)

    def _check_resize_vhd(self, vhd_path, vhd_info, new_size):
        curr_size = vhd_info['VirtualSize']
        if new_size < curr_size:
            raise exception.CannotResizeDisk(
                reason=_("Cannot resize the root disk to a smaller size. "
                         "Current size: %(curr_root_gb)s GB. Requested "
                         "size: %(new_root_gb)s GB.") % {
                             'curr_root_gb': curr_size / units.Gi,
                             'new_root_gb': new_size / units.Gi})
        elif new_size > curr_size:
            self._resize_vhd(vhd_path, new_size)

    def _resize_vhd(self, vhd_path, new_size):
        if vhd_path.split('.')[-1].lower() == "vhd":
            LOG.debug("Getting parent disk info for disk: %s", vhd_path)
            base_disk_path = self._vhdutils.get_vhd_parent_path(vhd_path)
            if base_disk_path:
                # A differential VHD cannot be resized. This limitation
                # does not apply to the VHDX format.
                self._merge_base_vhd(vhd_path, base_disk_path)
        LOG.debug("Resizing disk \"%(vhd_path)s\" to new max "
                  "size %(new_size)s",
                  {'vhd_path': vhd_path, 'new_size': new_size})
        self._vhdutils.resize_vhd(vhd_path, new_size)

    def _check_base_disk(self, context, instance, diff_vhd_path,
                         src_base_disk_path):
        base_vhd_path = self._imagecache.get_cached_image(context, instance)

        # If the location of the base host differs between source
        # and target hosts we need to reconnect the base disk
        if src_base_disk_path.lower() != base_vhd_path.lower():
            LOG.debug("Reconnecting copied base VHD "
                      "%(base_vhd_path)s and diff "
                      "VHD %(diff_vhd_path)s",
                      {'base_vhd_path': base_vhd_path,
                       'diff_vhd_path': diff_vhd_path})
            self._vhdutils.reconnect_parent_vhd(diff_vhd_path,
                                                base_vhd_path)

    def _migrate_disks_from_source(self, migration, instance,
                                   source_inst_dir):
        source_inst_dir = self._pathutils.get_remote_path(
            migration.source_compute, source_inst_dir)
        inst_dir = self._pathutils.get_instance_dir(
            instance.name, create_dir=True, remove_dir=True)

        self._pathutils.copy_folder_files(source_inst_dir, inst_dir)

    def finish_migration(self, context, migration, instance, disk_info,
                         network_info, image_meta, resize_instance=False,
                         block_device_info=None, power_on=True):
        LOG.debug("finish_migration called", instance=instance)
        self._migrate_disks_from_source(migration, instance, disk_info)

        vm_gen = self._vmops.get_image_vm_generation(instance.uuid, image_meta)
        self._check_and_update_disks(context, instance, vm_gen, image_meta,
                                     block_device_info,
                                     resize_instance=resize_instance)

        self._vmops.create_instance(context, instance, network_info,
                                    block_device_info, vm_gen, image_meta)

        self._check_and_attach_config_drive(instance, vm_gen)
        self._vmops.set_boot_order(instance.name, vm_gen, block_device_info)
        if power_on:
            self._vmops.power_on(instance, network_info=network_info)

    def _check_and_update_disks(self, context, instance, vm_gen, image_meta,
                                block_device_info, resize_instance=False):
        self._block_dev_man.validate_and_update_bdi(instance, image_meta,
                                                    vm_gen, block_device_info)
        root_device = block_device_info['root_disk']

        if root_device['type'] == constants.DISK:
            root_vhd_path = self._pathutils.lookup_root_vhd_path(instance.name)
            root_device['path'] = root_vhd_path
            if not root_vhd_path:
                base_vhd_path = self._pathutils.get_instance_dir(instance.name)
                raise exception.DiskNotFound(location=base_vhd_path)

            root_vhd_info = self._vhdutils.get_vhd_info(root_vhd_path)
            src_base_disk_path = root_vhd_info.get("ParentPath")
            if src_base_disk_path:
                self._check_base_disk(context, instance, root_vhd_path,
                                      src_base_disk_path)

            if resize_instance:
                new_size = instance.flavor.root_gb * units.Gi
                self._check_resize_vhd(root_vhd_path, root_vhd_info, new_size)

        ephemerals = block_device_info['ephemerals']
        self._check_ephemeral_disks(instance, ephemerals, resize_instance)

    def _check_ephemeral_disks(self, instance, ephemerals,
                               resize_instance=False):
        instance_name = instance.name
        new_eph_gb = instance.get('ephemeral_gb', 0)

        if len(ephemerals) == 1:
            # NOTE(claudiub): Resize only if there is one ephemeral. If there
            # are more than 1, resizing them can be problematic. This behaviour
            # also exists in the libvirt driver and it has to be addressed in
            # the future.
            ephemerals[0]['size'] = new_eph_gb
        elif sum(eph['size'] for eph in ephemerals) != new_eph_gb:
            # New ephemeral size is different from the original ephemeral size
            # and there are multiple ephemerals.
            LOG.warning(_LW("Cannot resize multiple ephemeral disks for "
                            "instance."), instance=instance)

        for index, eph in enumerate(ephemerals):
            eph_name = "eph%s" % index
            existing_eph_path = self._pathutils.lookup_ephemeral_vhd_path(
                instance_name, eph_name)

            if not existing_eph_path:
                eph['format'] = self._vhdutils.get_best_supported_vhd_format()
                eph['path'] = self._pathutils.get_ephemeral_vhd_path(
                    instance_name, eph['format'], eph_name)
                if not resize_instance:
                    # ephemerals should have existed.
                    raise exception.DiskNotFound(location=eph['path'])

                if eph['size']:
                    # create ephemerals
                    self._vmops.create_ephemeral_disk(instance.name, eph)
            elif eph['size'] > 0:
                # ephemerals exist. resize them.
                eph['path'] = existing_eph_path
                eph_vhd_info = self._vhdutils.get_vhd_info(eph['path'])
                self._check_resize_vhd(
                    eph['path'], eph_vhd_info, eph['size'] * units.Gi)
            else:
                # ephemeral new size is 0, remove it.
                self._pathutils.remove(existing_eph_path)
                eph['path'] = None
