# Copyright 2018 Cloudbase Solutions Srl
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

from nova import exception
from nova import objects
from nova.scheduler.client import report
from nova.scheduler import utils as scheduler_utils
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class PlacementUtils(object):
    def __init__(self):
        self.reportclient = report.SchedulerReportClient()

    def move_compute_node_allocations(self, context, instance, old_host,
                                      new_host):
        LOG.info("Moving instance allocations from compute node %s to %s.",
                 old_host, new_host, instance=instance)

        cn_uuid = objects.ComputeNode.get_by_host_and_nodename(
            context, old_host, old_host).uuid
        new_cn_uuid = objects.ComputeNode.get_by_host_and_nodename(
            context, new_host, new_host).uuid

        self.move_allocations(context, instance.uuid, cn_uuid,
                              new_cn_uuid, merge_existing=False)

    @report.retries
    def move_allocations(self, context, consumer_uuid, old_rp_uuid,
                         new_rp_uuid, merge_existing=True):
        alloc_data = self.reportclient.get_allocs_for_consumer(
            context, consumer_uuid)
        allocations = alloc_data['allocations']

        if old_rp_uuid == new_rp_uuid:
            LOG.debug("Requested to move allocations to the "
                      "same provider: %s.", old_rp_uuid)

        if old_rp_uuid not in allocations:
            LOG.warning("Expected to find allocations referencing resource "
                        "provider %s for %s, but found none.",
                        old_rp_uuid, consumer_uuid)
            return

        if merge_existing and new_rp_uuid in allocations:
            LOG.info("Merging existing allocations for consumer %s on "
                     "provider %s: %s.",
                     consumer_uuid, new_rp_uuid, allocations)
            scheduler_utils.merge_resources(
                allocations[new_rp_uuid]['resources'],
                allocations[old_rp_uuid]['resources'])
        else:
            if new_rp_uuid in allocations:
                LOG.info("Replacing existing allocations for consumer %s "
                         "on provider %s: %s",
                         consumer_uuid, new_rp_uuid, allocations)

            allocations[new_rp_uuid] = allocations[old_rp_uuid]

        del allocations[old_rp_uuid]

        self._put_allocations(context, consumer_uuid, alloc_data)

    def _put_allocations(self, context, consumer_uuid, allocations):
        url = '/allocations/%s' % consumer_uuid
        r = self.reportclient.put(url, allocations,
                                  version=report.CONSUMER_GENERATION_VERSION,
                                  global_request_id=context.global_id)
        if r.status_code != 204:
            err = r.json()['errors'][0]
            # NOTE(jaypipes): Yes, it sucks doing string comparison like this
            # but we have no error codes, only error messages.
            # TODO(gibi): Use more granular error codes when available
            if err['code'] == 'placement.concurrent_update':
                if 'consumer generation conflict' in err['detail']:
                    raise exception.AllocationUpdateFailed(
                        consumer_uuid=consumer_uuid, error=err['detail'])
                # this is not a consumer generation conflict so it can only be
                # a resource provider generation conflict. The caller does not
                # provide resource provider generation so this is just a
                # placement internal race. We can blindly retry locally.
                reason = ('another process changed the resource providers '
                          'involved in our attempt to put allocations for '
                          'consumer %s' % consumer_uuid)
                raise report.Retry('put_allocations', reason)
            else:
                LOG.warning(
                    'Unable to submit allocation for instance '
                    '%(uuid)s (%(code)i %(text)s)',
                    {'uuid': consumer_uuid,
                     'code': r.status_code,
                     'text': r.text})
        return r.status_code == 204
