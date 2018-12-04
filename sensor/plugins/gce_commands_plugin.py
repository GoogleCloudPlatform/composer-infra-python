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

import datetime
import logging
from airflow.models import BaseOperator
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import googleapiclient.discovery
from oauth2client.client import GoogleCredentials


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
    # [START stop_oper_xcom]
    operation = self.compute.instances().stop(
        project=self.project, zone=self.zone, instance=self.instance).execute()
    return operation['name']
    # [END stop_oper_xcom]


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
    # [START snap_oper_xcom]
    operation = self.compute.disks().createSnapshot(
        project=self.project, zone=self.zone, disk=self.disk,
        body={'name': snapshot_name}).execute()
    return operation['name']
    # [END snap_oper_xcom]


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
    # [START start_oper_xcom]
    operation = self.compute.instances().start(
        project=self.project, zone=self.zone, instance=self.instance).execute()
    return operation['name']
    # [END start_oper_xcom]


class OperationStatusSensor(BaseSensorOperator):
  """Waits for a Compute Engine operation to complete."""

  @apply_defaults
  def __init__(self, project, zone, instance, prior_task_id, *args, **kwargs):
    self.compute = self.get_compute_api_client()
    self.project = project
    self.zone = zone
    self.instance = instance
    self.prior_task_id = prior_task_id
    super(OperationStatusSensor, self).__init__(*args, **kwargs)

  def get_compute_api_client(self):
    credentials = GoogleCredentials.get_application_default()
    return googleapiclient.discovery.build(
        'compute', 'v1', cache_discovery=False, credentials=credentials)

  def poke(self, context):
    operation_name = context['task_instance'].xcom_pull(
        task_ids=self.prior_task_id)
    result = self.compute.zoneOperations().get(
        project=self.project, zone=self.zone,
        operation=operation_name).execute()

    logging.info(
        "Task '%s' current status: '%s'", self.prior_task_id, result['status'])
    if result['status'] == 'DONE':
      return True
    else:
      logging.info("Waiting for task '%s' to complete", self.prior_task_id)
      return False


class GoogleComputeEnginePlugin(AirflowPlugin):
  """Expose Airflow operators and sensor."""

  name = 'gce_commands_plugin'
  operators = [StopInstanceOperator, SnapshotDiskOperator,
               StartInstanceOperator, OperationStatusSensor]
