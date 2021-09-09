# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Airflow Plugin to backup a Compute Engine virtual machine instance."""

# [START imports]
import datetime
import logging
import time
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials
# [END imports]


class StopInstanceOperator(BaseOperator):
  """Stops the virtual machine instance."""

  @apply_defaults
  def __init__(self, project, zone, instance, *args, **kwargs):
    self.compute = self.get_compute_api_client()
    self.project = project
    self.zone = zone
    self.instance = instance
    super(StopInstanceOperator, self).__init__(*args, **kwargs)

  def get_compute_api_client(self):
    credentials = GoogleCredentials.get_application_default()
    return googleapiclient.discovery.build(
        'compute', 'v1', cache_discovery=False, credentials=credentials)

  def execute(self, context):
    logging.info('Stopping instance %s in project %s and zone %s',
                 self.instance, self.project, self.zone)
    # [START stop_oper_no_xcom]
    self.compute.instances().stop(
        project=self.project, zone=self.zone, instance=self.instance).execute()
    time.sleep(90)
    # [END stop_oper_no_xcom]


class SnapshotDiskOperator(BaseOperator):
  """Takes a snapshot of a persistent disk."""

  @apply_defaults
  def __init__(self, project, zone, instance, disk, *args, **kwargs):
    self.compute = self.get_compute_api_client()
    self.project = project
    self.zone = zone
    self.instance = instance
    self.disk = disk
    super(SnapshotDiskOperator, self).__init__(*args, **kwargs)

  def get_compute_api_client(self):
    credentials = GoogleCredentials.get_application_default()
    return googleapiclient.discovery.build(
        'compute', 'v1', cache_discovery=False, credentials=credentials)

  def generate_snapshot_name(self, instance):
    # Snapshot name must match regex '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'
    return ('' + self.instance + '-' +
            datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))

  def execute(self, context):
    snapshot_name = self.generate_snapshot_name(self.instance)
    logging.info(
        ("Creating snapshot '%s' from: {disk=%s, instance=%s, project=%s, "
         "zone=%s}"),
        snapshot_name, self.disk, self.instance, self.project, self.zone)
    # [START snap_oper_no_xcom]
    self.compute.disks().createSnapshot(
        project=self.project, zone=self.zone, disk=self.disk,
        body={'name': snapshot_name}).execute()
    time.sleep(120)
    # [END snap_oper_no_xcom]


class StartInstanceOperator(BaseOperator):
  """Starts a virtual machine instance."""

  @apply_defaults
  def __init__(self, project, zone, instance, *args, **kwargs):
    self.compute = self.get_compute_api_client()
    self.project = project
    self.zone = zone
    self.instance = instance
    super(StartInstanceOperator, self).__init__(*args, **kwargs)

  def get_compute_api_client(self):
    credentials = GoogleCredentials.get_application_default()
    return googleapiclient.discovery.build(
        'compute', 'v1', cache_discovery=False, credentials=credentials)

  def execute(self, context):
    logging.info('Starting instance %s in project %s and zone %s',
                 self.instance, self.project, self.zone)
    # [START start_oper_no_xcom]
    self.compute.instances().start(
        project=self.project, zone=self.zone, instance=self.instance).execute()
    time.sleep(20)
    # [END start_oper_no_xcom]
