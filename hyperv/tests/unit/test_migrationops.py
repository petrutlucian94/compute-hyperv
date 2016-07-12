#  Copyright 2014 IBM Corp.
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

import ddt
import os

import mock
import nova.conf
from nova import exception
from os_win import exceptions as os_win_exc
from oslo_utils import units

from hyperv.nova import constants
from hyperv.nova import migrationops
from hyperv.tests import fake_instance
from hyperv.tests.unit import test_base

CONF = nova.conf.CONF


@ddt.ddt
class MigrationOpsTestCase(test_base.HyperVBaseTestCase):
    """Unit tests for the Hyper-V MigrationOps class."""

    _FAKE_DISK = 'fake_disk'
    _FAKE_TIMEOUT = 10
    _FAKE_RETRY_INTERVAL = 5

    def setUp(self):
        super(MigrationOpsTestCase, self).setUp()
        self.context = 'fake-context'

        self._migrationops = migrationops.MigrationOps()
        self._migrationops._hostutils = mock.MagicMock()
        self._migrationops._vmops = mock.MagicMock()
        self._migrationops._vmutils = mock.MagicMock()
        self._migrationops._pathutils = mock.Mock()
        self._migrationops._vhdutils = mock.MagicMock()
        self._migrationops._pathutils = mock.MagicMock()
        self._migrationops._volumeops = mock.MagicMock()
        self._migrationops._imagecache = mock.MagicMock()
        self._migrationops._block_dev_man = mock.MagicMock()
        self._migrationops._migrationutils = mock.MagicMock()
        self._pathutils = self._migrationops._pathutils
        self._vhdutils = self._migrationops._vhdutils
        self._vmops = self._migrationops._vmops
        self._vmutils = self._migrationops._vmutils
        self._imagecache = self._migrationops._imagecache
        self._volumeops = self._migrationops._volumeops

    def _check_migrate_disk_files(self, shared_storage=False):
        instance_path = 'fake/instance/path'
        dest_instance_path = 'remote/instance/path'
        self._pathutils.get_instance_dir.side_effect = (
            instance_path, dest_instance_path)
        self._pathutils.check_dirs_shared_storage.return_value = shared_storage
        self._pathutils.exists.return_value = True

        fake_disk_files = [os.path.join(instance_path, disk_name)
                           for disk_name in
                           ['root.vhd', 'configdrive.vhd', 'configdrive.iso',
                            'eph0.vhd', 'eph1.vhdx']]

        expected_get_dir = [mock.call(mock.sentinel.instance_name),
                            mock.call(mock.sentinel.instance_name,
                                      mock.sentinel.dest_path)]
        expected_move_calls = [mock.call(
            instance_path,
            self._pathutils.get_instance_migr_revert_dir.return_value)]

        self._migrationops._migrate_disk_files(
            instance_name=mock.sentinel.instance_name,
            disk_files=fake_disk_files,
            dest=mock.sentinel.dest_path)

        self._pathutils.get_instance_dir.assert_has_calls(expected_get_dir)
        self._pathutils.get_instance_migr_revert_dir.assert_called_with(
            mock.sentinel.instance_name, remove_dir=True, create_dir=True)
        self._pathutils.exists.assert_called_once_with(dest_instance_path)
        self._pathutils.check_dirs_shared_storage.assert_called_once_with(
            instance_path, dest_instance_path)
        if shared_storage:
            dest_instance_path = '%s_tmp' % instance_path
        remove_dir_calls = [mock.call(dest_instance_path)]

        if shared_storage:
            expected_move_calls.append(mock.call(dest_instance_path,
                                                 instance_path))
            self._pathutils.rmtree.assert_called_once_with(dest_instance_path)

        self._pathutils.makedirs.assert_called_once_with(dest_instance_path)
        self._pathutils.check_remove_dir.assert_has_calls(remove_dir_calls)

        self._pathutils.get_instance_dir.assert_has_calls(expected_get_dir)
        self._pathutils.copy.assert_has_calls(
            mock.call(fake_disk_file, dest_instance_path)
            for fake_disk_file in fake_disk_files)
        self.assertEqual(len(fake_disk_files), self._pathutils.copy.call_count)
        self._pathutils.copy_acls.assert_has_calls(
            mock.call(fake_disk_file,
                      os.path.join(dest_instance_path,
                                   os.path.basename(fake_disk_file)))
            for fake_disk_file in fake_disk_files)
        self._pathutils.move_folder_files.assert_has_calls(expected_move_calls)

    def test_migrate_disk_files(self):
        self._check_migrate_disk_files()

    def test_migrate_disk_files_same_host(self):
        self._check_migrate_disk_files(shared_storage=True)

    @mock.patch.object(migrationops.MigrationOps,
                       '_cleanup_failed_disk_migration')
    def test_migrate_disk_files_exception(self, mock_cleanup):
        instance_path = 'fake/instance/path'
        fake_dest_path = '%s_tmp' % instance_path
        self._pathutils.get_instance_dir.return_value = instance_path
        self._migrationops._hostutils.get_local_ips.return_value = [
            mock.sentinel.dest_path]
        self._pathutils.copy.side_effect = IOError(
            "Expected exception.")

        self.assertRaises(IOError, self._migrationops._migrate_disk_files,
                          instance_name=mock.sentinel.instance_name,
                          disk_files=[self._FAKE_DISK],
                          dest=mock.sentinel.dest_path)
        mock_cleanup.assert_called_once_with(
            instance_path,
            self._pathutils.get_instance_migr_revert_dir.return_value,
            fake_dest_path)

    def test_cleanup_failed_disk_migration(self):
        self._pathutils.exists.return_value = True

        self._migrationops._cleanup_failed_disk_migration(
            instance_path=mock.sentinel.instance_path,
            revert_path=mock.sentinel.revert_path,
            dest_path=mock.sentinel.dest_path)

        expected = [mock.call(mock.sentinel.dest_path),
                    mock.call(mock.sentinel.revert_path)]
        self._pathutils.exists.assert_has_calls(expected)
        self._pathutils.move_folder_files.assert_called_once_with(
            mock.sentinel.revert_path, mock.sentinel.instance_path)
        self._pathutils.rmtree.assert_has_calls([
            mock.call(mock.sentinel.dest_path),
            mock.call(mock.sentinel.revert_path)])

    def test_check_target_flavor(self):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_instance.flavor.root_gb = 1
        mock_flavor = mock.MagicMock(root_gb=0)
        self.assertRaises(exception.InstanceFaultRollback,
                          self._migrationops._check_target_flavor,
                          mock_instance, mock_flavor)

    def test_check_and_attach_config_drive(self):
        mock_instance = fake_instance.fake_instance_obj(
            self.context, expected_attrs=['system_metadata'])
        mock_instance.config_drive = 'True'

        self._migrationops._check_and_attach_config_drive(
            mock_instance, mock.sentinel.vm_gen)

        self._migrationops._vmops.attach_config_drive.assert_called_once_with(
            mock_instance,
            self._pathutils.lookup_configdrive_path.return_value,
            mock.sentinel.vm_gen)

    def test_check_and_attach_config_drive_unknown_path(self):
        instance = fake_instance.fake_instance_obj(
            self.context, expected_attrs=['system_metadata'])
        instance.config_drive = 'True'
        self._pathutils.lookup_configdrive_path.return_value = None
        self.assertRaises(exception.ConfigDriveNotFound,
                          self._migrationops._check_and_attach_config_drive,
                          instance,
                          mock.sentinel.FAKE_VM_GEN)

    @mock.patch.object(migrationops.MigrationOps, '_export_vm_to_destination')
    @mock.patch.object(migrationops.MigrationOps, '_check_target_flavor')
    def _test_migrate_disk_and_power_off(self, mock_check_flavor,
                                         mock_export_vm_to_destination):
        mock_check_remote_instances_dir_shared = (
            self._pathutils.check_remote_instances_dir_shared)
        instance = fake_instance.fake_instance_obj(self.context)
        flavor = mock.MagicMock()
        network_info = mock.MagicMock()
        mock_check_remote_instances_dir_shared.return_value = (
            mock.sentinel.shared_storage)

        self._migrationops.migrate_disk_and_power_off(
            self.context, instance, mock.sentinel.fake_dest, flavor,
            network_info, mock.sentinel.block_device_info, self._FAKE_TIMEOUT,
            self._FAKE_RETRY_INTERVAL)

        mock_check_remote_instances_dir_shared.assert_called_once_with(
            mock.sentinel.fake_dest)
        mock_check_flavor.assert_called_once_with(instance, flavor)
        self._migrationops._vmops.power_off.assert_called_once_with(
            instance, self._FAKE_TIMEOUT, self._FAKE_RETRY_INTERVAL)
        mock_export_vm_to_destination.assert_called_once_with(
            instance, mock.sentinel.fake_dest, mock.sentinel.block_device_info,
            shared_storage=mock.sentinel.shared_storage)
        self._migrationops._vmops.destroy.assert_called_once_with(
                instance, destroy_disks=False)

    def test_confirm_migration(self):
        mock_instance = fake_instance.fake_instance_obj(self.context)

        self._migrationops.confirm_migration(
            migration=mock.sentinel.migration, instance=mock_instance,
            network_info=mock.sentinel.network_info)

        self._pathutils.get_instance_migr_revert_dir.assert_called_with(
            mock_instance.name, remove_dir=True)
        self._pathutils.get_export_dir.assert_called_with(
            mock_instance.name, create_dir=False, remove_dir=True)

    def test_revert_migration_files(self):
        instance_path = self._pathutils.get_instance_dir.return_value
        revert_path = self._pathutils.get_instance_migr_revert_dir.return_value

        self._migrationops._revert_migration_files(
            instance_name=mock.sentinel.instance_name)

        self._pathutils.get_instance_dir.assert_called_once_with(
            mock.sentinel.instance_name, create_dir=False, remove_dir=True)
        self._pathutils.get_instance_migr_revert_dir.assert_called_once_with(
            mock.sentinel.instance_name)
        self._pathutils.rename.assert_called_once_with(
            revert_path, instance_path)

    @mock.patch.object(migrationops.MigrationOps, '_import_and_setup_vm')
    @mock.patch.object(migrationops.MigrationOps, '_check_and_update_disks')
    @mock.patch.object(migrationops.MigrationOps, '_revert_migration_files')
    def test_finish_revert_migration(self, mock_revert_migration_files,
                                     mock_check_and_update_disks,
                                     mock_import_and_setup_vm):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        image_meta = self._imagecache.get_image_details.return_value

        self._migrationops.finish_revert_migration(
            context=self.context, instance=mock_instance,
            network_info=mock.sentinel.network_info,
            block_device_info=mock.sentinel.block_device_info,
            power_on=True)

        mock_revert_migration_files.assert_called_once_with(
            mock_instance.name)
        self._imagecache.get_image_details.assert_called_once_with(
            self.context, mock_instance)
        mock_import_and_setup_vm.assert_called_once_with(
            mock_instance, image_meta, mock.sentinel.block_device_info)
        mock_check_and_update_disks.assert_called_once_with(
            self.context, mock_instance, image_meta,
            mock.sentinel.block_device_info)
        self._pathutils.get_export_dir.assert_called_once_with(
            instance_name=mock_instance.name,
            create_dir=False,
            remove_dir=True)
        self._vmops.power_on.assert_called_once_with(
            mock_instance, network_info=mock.sentinel.network_info)

    def test_merge_base_vhd(self):
        fake_diff_vhd_path = 'fake/diff/path'
        fake_base_vhd_path = 'fake/base/path'
        base_vhd_copy_path = os.path.join(
            os.path.dirname(fake_diff_vhd_path),
            os.path.basename(fake_base_vhd_path))

        self._migrationops._merge_base_vhd(diff_vhd_path=fake_diff_vhd_path,
                                           base_vhd_path=fake_base_vhd_path)

        self._pathutils.copyfile.assert_called_once_with(
            fake_base_vhd_path, base_vhd_copy_path)
        recon_parent_vhd = self._migrationops._vhdutils.reconnect_parent_vhd
        recon_parent_vhd.assert_called_once_with(fake_diff_vhd_path,
                                                 base_vhd_copy_path)
        self._migrationops._vhdutils.merge_vhd.assert_called_once_with(
            fake_diff_vhd_path)
        self._pathutils.rename.assert_called_once_with(
            base_vhd_copy_path, fake_diff_vhd_path)

    def test_merge_base_vhd_exception(self):
        fake_diff_vhd_path = 'fake/diff/path'
        fake_base_vhd_path = 'fake/base/path'
        base_vhd_copy_path = os.path.join(
            os.path.dirname(fake_diff_vhd_path),
            os.path.basename(fake_base_vhd_path))

        self._migrationops._vhdutils.reconnect_parent_vhd.side_effect = (
            os_win_exc.HyperVException)
        self._pathutils.exists.return_value = True

        self.assertRaises(os_win_exc.HyperVException,
                          self._migrationops._merge_base_vhd,
                          fake_diff_vhd_path, fake_base_vhd_path)
        self._pathutils.exists.assert_called_once_with(
            base_vhd_copy_path)
        self._pathutils.remove.assert_called_once_with(
            base_vhd_copy_path)

    @mock.patch.object(migrationops.MigrationOps, '_resize_vhd')
    def test_check_resize_vhd(self, mock_resize_vhd):
        self._migrationops._check_resize_vhd(
            vhd_path=mock.sentinel.vhd_path, vhd_info={'VirtualSize': 1},
            new_size=2)
        mock_resize_vhd.assert_called_once_with(mock.sentinel.vhd_path, 2)

    def test_check_resize_vhd_exception(self):
        self.assertRaises(exception.CannotResizeDisk,
                          self._migrationops._check_resize_vhd,
                          mock.sentinel.vhd_path,
                          {'VirtualSize': 1}, 0)

    @mock.patch.object(migrationops.MigrationOps, '_merge_base_vhd')
    def test_resize_vhd(self, mock_merge_base_vhd):
        fake_vhd_path = 'fake/path.vhd'
        new_vhd_size = 2
        self._migrationops._resize_vhd(vhd_path=fake_vhd_path,
                                       new_size=new_vhd_size)

        get_vhd_parent_path = self._migrationops._vhdutils.get_vhd_parent_path
        get_vhd_parent_path.assert_called_once_with(fake_vhd_path)
        mock_merge_base_vhd.assert_called_once_with(
            fake_vhd_path,
            self._migrationops._vhdutils.get_vhd_parent_path.return_value)
        self._migrationops._vhdutils.resize_vhd.assert_called_once_with(
            fake_vhd_path, new_vhd_size)

    def test_check_base_disk(self):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        fake_src_vhd_path = 'fake/src/path'
        fake_base_vhd = 'fake/vhd'
        get_cached_image = self._migrationops._imagecache.get_cached_image
        get_cached_image.return_value = fake_base_vhd

        self._migrationops._check_base_disk(
            context=self.context, instance=mock_instance,
            diff_vhd_path=mock.sentinel.diff_vhd_path,
            src_base_disk_path=fake_src_vhd_path)

        get_cached_image.assert_called_once_with(self.context, mock_instance)
        recon_parent_vhd = self._migrationops._vhdutils.reconnect_parent_vhd
        recon_parent_vhd.assert_called_once_with(
            mock.sentinel.diff_vhd_path, fake_base_vhd)

    @mock.patch.object(migrationops.MigrationOps, '_import_and_setup_vm')
    @mock.patch.object(migrationops.MigrationOps, '_check_and_update_disks')
    def test_finish_migration(self, mock_check_and_update_disks,
                              mock_import_and_setup_vm):
        mock_instance = fake_instance.fake_instance_obj(self.context)

        self._migrationops.finish_migration(
            context=self.context, migration=mock.sentinel.migration,
            instance=mock_instance, disk_info=mock.sentinel.disk_info,
            network_info=mock.sentinel.network_info,
            image_meta=mock.sentinel.image_meta, resize_instance=True,
            block_device_info=mock.sentinel.block_device_info)

        mock_import_and_setup_vm.assert_called_once_with(
            mock_instance, mock.sentinel.image_meta,
            mock.sentinel.block_device_info)
        mock_check_and_update_disks.assert_called_once_with(
            self.context, mock_instance, mock.sentinel.image_meta,
            mock.sentinel.block_device_info, resize_instance=True)

        self._migrationops._vmops.power_on.assert_called_once_with(
            mock_instance)

    @mock.patch.object(migrationops.MigrationOps, '_check_resize_vhd')
    @mock.patch.object(migrationops.LOG, 'warning')
    def test_check_ephemeral_disks_multiple_eph_warn(self, mock_warn,
                                                     mock_check_resize_vhd):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_instance.ephemeral_gb = 3
        mock_ephemerals = [{'size': 1}, {'size': 1}]
        block_device_info = {'ephemerals': mock_ephemerals}

        self._migrationops._check_ephemeral_disks(mock_instance,
                                                  block_device_info,
                                                  mock.sentinel.image_meta,
                                                  True)

    def test_check_ephemeral_disks_exception(self):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_ephemerals = [dict()]
        block_device_info = {'ephemerals': mock_ephemerals}
        self._pathutils.lookup_ephemeral_vhd_path.return_value = None

        self.assertRaises(exception.DiskNotFound,
                          self._migrationops._check_ephemeral_disks,
                          mock_instance, block_device_info,
                          mock.sentinel.image_meta)

    @mock.patch.object(migrationops.MigrationOps, '_check_resize_vhd')
    def _test_check_ephemeral_disks(self, mock_check_resize_vhd,
                                    existing_eph_path=None, new_eph_size=42):
        mock_validate_and_update_bdi = (
            self._migrationops._block_dev_man.validate_and_update_bdi)
        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_instance.ephemeral_gb = new_eph_size
        eph = {}
        mock_ephemerals = [eph]
        block_device_info = {'ephemerals': mock_ephemerals}

        self._pathutils.lookup_ephemeral_vhd_path.return_value = (
            existing_eph_path)
        self._pathutils.get_ephemeral_vhd_path.return_value = (
            mock.sentinel.get_path)
        self._vmops.get_image_vm_generation.return_value = mock.sentinel.vm_gen
        self._vhdutils.get_best_supported_vhd_format.return_value = (
            mock.sentinel.vhd_format)

        self._migrationops._check_ephemeral_disks(mock_instance,
                                                  block_device_info,
                                                  mock.sentinel.image_meta,
                                                  True)

        self.assertEqual(mock_instance.ephemeral_gb, eph['size'])
        if not existing_eph_path:
            self._vmops.create_ephemeral_disk.assert_called_once_with(
                mock_instance.name, eph)
            self._vmops.attach_ephemerals.assert_called_once_with(
                mock_instance.name, mock_ephemerals)
            mock_validate_and_update_bdi.assert_called_once_with(
                mock_instance, mock.sentinel.image_meta,
                mock.sentinel.vm_gen, block_device_info)
            self.assertEqual(mock.sentinel.vhd_format, eph['format'])
            self.assertEqual(mock.sentinel.get_path, eph['path'])
        elif new_eph_size:
            mock_check_resize_vhd.assert_called_once_with(
                existing_eph_path,
                self._vhdutils.get_vhd_info.return_value,
                mock_instance.ephemeral_gb * units.Gi)
            self.assertEqual(existing_eph_path, eph['path'])
        else:
            self._pathutils.detach_vm_disk(
                mock_instance.name, existing_eph_path, is_physical=False)
            self._pathutils.remove.assert_called_once_with(existing_eph_path)

    def test_check_ephemeral_disks_create(self):
        self._test_check_ephemeral_disks()

    def test_check_ephemeral_disks_resize(self):
        self._test_check_ephemeral_disks(existing_eph_path=mock.sentinel.path)

    def test_check_ephemeral_disks_remove(self):
        self._test_check_ephemeral_disks(existing_eph_path=mock.sentinel.path,
                                         new_eph_size=0)

    # Tests the scenario in which no ephemeral disk is explicitly
    # passed yet the ephemeral disk size is greater than 0.
    @mock.patch.object(migrationops.MigrationOps, '_check_resize_vhd')
    def test_check_ephemeral_disks_add(self, mock_check_resize_vhd):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        new_eph_gb = 1
        mock_instance.ephemeral_gb = new_eph_gb
        block_device_info = {'ephemerals': []}
        mock_instance = fake_instance.fake_instance_obj(self.context)
        self._vmops.get_image_vm_generation.return_value = mock.sentinel.vm_gen
        mock_instance.ephemeral_gb = new_eph_gb
        new_eph = {'size': new_eph_gb,
                   'device_name': None,
                   'disk_bus': None}
        self._pathutils.lookup_ephemeral_vhd_path.return_value = None
        new_eph['format'] = (
            self._vhdutils.get_best_supported_vhd_format.return_value)
        new_eph['path'] = self._pathutils.get_ephemeral_vhd_path.return_value

        self._migrationops._check_ephemeral_disks(mock_instance,
                                                  block_device_info,
                                                  mock.sentinel.image_meta,
                                                  True)
        self._vmops.create_ephemeral_disk.assert_called_once_with(
            mock_instance.name, new_eph)
        self._migrationops._block_dev_man.validate_and_update_bdi(
            mock_instance, mock.sentinel.image_meta,
            mock.sentinel.vm_gen, block_device_info)
        self._vmops.attach_ephemerals.assert_called_once_with(
            mock_instance.name, [new_eph])
        mock_check_resize_vhd(new_eph['path'],
                              self._vhdutils.get_vhd_info,
                              new_eph['size'] * units.Gi)
        self.assertEqual([new_eph], block_device_info['ephemerals'])

    @ddt.data(True, False)
    @mock.patch.object(migrationops.MigrationOps, '_migrate_disk_files')
    def test_export_vm_to_destiantion(self, shared_storage,
                                      mock_migrate_disk_files):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        self._vmutils.get_vm_storage_paths.return_value = (
            mock.sentinel.disk_files, mock.sentinel.volume_drives)
        self._pathutils.get_export_dir.side_effect = [
            mock.sentinel.local_exp_path, mock.sentinel.dest_exp_path]

        self._migrationops._export_vm_to_destination(
            mock_instance, mock.sentinel.dest,
            mock.sentinel.block_device_info, shared_storage=shared_storage)

        self._vmutils.get_vm_storage_paths.assert_called_once_with(
            mock_instance.name)
        self._migrationops._migrationutils.export_vm.assert_called_once_with(
            mock_instance.name, mock.sentinel.local_exp_path)
        mock_migrate_disk_files.assert_called_once_with(mock_instance.name,
            mock.sentinel.disk_files, mock.sentinel.dest)
        mock_exp_calls = [mock.call(mock_instance.name,
                                    create_dir=False, remove_dir=False)]
        if not shared_storage:
            mock_exp_calls.append(
                mock.call(mock_instance.name, mock.sentinel.dest,
                          create_dir=False, remove_dir=False))
            self._pathutils.copy_dir.assert_called_once_with(
                mock.sentinel.local_exp_path, mock.sentinel.dest_exp_path)
        else:
            self.assertFalse(self._migrationops._pathutils.copy_dir.called)
        self._pathutils.get_export_dir.assert_has_calls(mock_exp_calls)

    def test_import_vm(self):
        self._migrationops._import_vm(mock.sentinel.vm_name)

        self._pathutils.get_export_snapshot_dir.assert_called_once_with(
            mock.sentinel.vm_name)
        self._pathutils.get_vm_config_file_path.assert_called_once_with(
            mock.sentinel.vm_name)
        mock_import_vm_definition = (
            self._migrationops._migrationutils.import_vm_definition)
        mock_import_vm_definition.assert_called_once_with(
            self._pathutils.get_vm_config_file_path.return_value,
            self._pathutils.get_export_snapshot_dir.return_value)

    @mock.patch.object(migrationops.MigrationOps, '_update_disk_image_paths')
    @mock.patch.object(migrationops.MigrationOps, '_import_vm')
    def test_import_and_setup_vm(self, mock_import_vm,
                                 mock_update_disk_image_paths):
        mock_check_remote_instances_dir_shared = (
            self._pathutils.check_remote_instances_dir_shared)
        self._vmops._get_instance_vnuma_config.return_value = (
            mock.sentinel.mem_per_numa_node, mock.sentinel.vnuma_vcpus)
        self._vmops.get_instance_dynamic_memory_ratio.return_value = (
            mock.sentinel.dynamic_memory_ratio)
        configuration_root_dir = self._pathutils.get_instance_dir.return_value
        snapshot_dir = self._pathutils.get_instance_snapshot_dir.return_value

        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_check_remote_instances_dir_shared.return_value = False

        self._migrationops._import_and_setup_vm(
            mock_instance, mock.sentinel.image_meta,
            mock.sentinel.block_device_info)

        mock_check_remote_instances_dir_shared.assert_called_once_with(
            mock_instance.launched_on)
        self._pathutils.get_instance_dir.assert_called_once_with(
            mock_instance.name)
        self._pathutils.get_instance_snapshot_dir.assert_called_once_with(
            mock_instance.name)
        mock_import_vm.assert_called_once_with(mock_instance.name)
        self._migrationops._volumeops.connect_volumes.assert_called_once_with(
            mock.sentinel.block_device_info)
        self._vmops._get_instance_vnuma_config.assert_called_once_with(
            mock_instance, mock.sentinel.image_meta)
        self._vmops.get_instance_dynamic_memory_ratio.assert_called_once_with(
            True)
        self._vmutils.update_vm.assert_called_once_with(
            vm_name=mock_instance.name,
            memory_mb=mock_instance.flavor.memory_mb,
            memory_per_numa_node=mock.sentinel.mem_per_numa_node,
            vcpus_num=mock_instance.flavor.vcpus,
            vcpus_per_numa_node=mock.sentinel.vnuma_vcpus,
            limit_cpu_features=CONF.hyperv.limit_cpu_features,
            dynamic_mem_ratio=mock.sentinel.dynamic_memory_ratio,
            configuration_root_dir=configuration_root_dir,
            snapshot_dir=snapshot_dir,
            is_planned_vm=True)
        self._volumeops.fix_instance_volume_disk_paths.assert_called_once_with(
            mock_instance.name, mock.sentinel.block_device_info,
            is_planned_vm=True)
        mock_update_disk_image_paths.assert_called_once_with(
            mock_instance.name, is_planned_vm=True)
        self._pathutils.get_export_dir.assert_called_once_with(
            instance_name=mock_instance.name, create_dir=False,
            remove_dir=True)
        self._migrationops._migrationutils.realize_vm.assert_called_once_with(
            mock_instance.name)

    @ddt.data(constants.DISK, mock.sentinel.root_type)
    @mock.patch.object(migrationops.MigrationOps, '_check_base_disk')
    @mock.patch.object(migrationops.MigrationOps, '_check_resize_vhd')
    @mock.patch.object(migrationops.MigrationOps, '_check_ephemeral_disks')
    def test_check_and_update_disks(self, root_type,
                                    mock_check_eph_disks,
                                    mock_check_resize_vhd,
                                    mock_check_base_disk):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        mock_instance.ephemeral_gb = 1
        root_device = {'type': root_type}
        block_device_info = {'root_disk': root_device, 'ephemerals': []}

        mock_vhd_info = self._vhdutils.get_vhd_info.return_value

        expected_check_resize = []
        expected_get_info = []

        self._migrationops._check_and_update_disks(
            context=self.context, instance=mock_instance,
            image_meta=mock.sentinel.image_meta,
            block_device_info=block_device_info,
            resize_instance=True)

        self._vmops.get_image_vm_generation.assert_called_once_with(
            mock_instance.uuid, mock.sentinel.image_meta)
        mock_bdi = self._migrationops._block_dev_man.validate_and_update_bdi
        mock_bdi.assert_called_once_with(mock_instance,
            mock.sentinel.image_meta,
            self._vmops.get_image_vm_generation.return_value,
            block_device_info)
        if root_device['type'] == constants.DISK:
            root_device_path = (
                self._pathutils.lookup_root_vhd_path.return_value)
            self._pathutils.lookup_root_vhd_path.assert_called_once_with(
                mock_instance.name)
            expected_get_info = [mock.call(root_device_path)]
            mock_vhd_info.get.assert_called_once_with("ParentPath")
            mock_check_base_disk.assert_called_once_with(
                self.context, mock_instance, root_device_path,
                mock_vhd_info.get.return_value)
            expected_check_resize.append(
                mock.call(root_device_path, mock_vhd_info,
                          mock_instance.root_gb * units.Gi))
        else:
            self.assertFalse(self._pathutils.lookup_root_vhd.called)
        mock_check_eph_disks.assert_called_once_with(
            mock_instance, block_device_info, mock.sentinel.image_meta, True)

        mock_check_resize_vhd.assert_has_calls(expected_check_resize)
        self._migrationops._vhdutils.get_vhd_info.assert_has_calls(
            expected_get_info)

    def test_check_and_update_no_root_disk(self):
        mock_instance = fake_instance.fake_instance_obj(self.context)
        self._pathutils.lookup_root_vhd_path.return_value = None
        bdi = {'root_disk': {'type': constants.DISK},
               'ephemerals': []}

        self.assertRaises(
            exception.DiskNotFound,
            self._migrationops._check_and_update_disks, self.context,
            mock_instance, mock.sentinel.image_meta, bdi,
            mock.sentinel.resize_instance)

    @mock.patch('os.path.exists')
    def test_update_disk_image_paths(self, mock_exists):
        disk_files = ["c:/instance/config/configdrive.iso"]
        inst_dir = "c:/instance"

        self._vmutils.get_vm_storage_paths.return_value = (
            disk_files, mock.sentinel.volume_drives)
        self._pathutils.get_instance_dir.return_value = inst_dir
        mock_exists.return_value = True

        self._migrationops._update_disk_image_paths(
            mock.sentinel.vm_name, is_planned_vm=False)

        self._vmutils.get_vm_storage_paths.assert_called_once_with(
            mock.sentinel.vm_name, is_planned_vm=False)
        self._vmutils.update_vm_disk_path.assert_called_once_with(
            disk_files[0], "c:/instance/configdrive.iso", is_physical=False)

    @mock.patch('os.path.exists')
    def test_update_disk_image_paths_exception(self, mock_exists):
        disk_files = ["c:/instance/root.vhdx"]
        inst_dir = "c:/instance"

        self._vmutils.get_vm_storage_paths.return_value = (
            disk_files, mock.sentinel.volume_drives)
        self._pathutils.get_instance_dir.return_value = inst_dir
        mock_exists.return_value = False

        self.assertRaises(exception.DiskNotFound,
                          self._migrationops._update_disk_image_paths,
                          mock.sentinel.vm_name, is_planned_vm=False)

        self._vmutils.get_vm_storage_paths.assert_called_once_with(
            mock.sentinel.vm_name, is_planned_vm=False)
        self.assertFalse(self._vmutils.update_vm_disk_path.called)
