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

"""Airflow DAG to backup a Compute Engine virtual machine instance."""

import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators import OperationStatusSensor
from airflow.operators import SnapshotDiskOperator
from airflow.operators import StartInstanceOperator
from airflow.operators import StopInstanceOperator
from airflow.operators.dummy_operator import DummyOperator

INTERVAL = '@daily'
START_DATE = datetime.datetime(2018, 7, 16)
PROJECT = Variable.get('PROJECT')
ZONE = Variable.get('ZONE')
INSTANCE = Variable.get('INSTANCE')
DISK = Variable.get('DISK')

# Airflow operators definition
dag1 = DAG('backup_vm_instance',
           description='Backup a Compute Engine instance using an Airflow DAG',
           schedule_interval=INTERVAL,
           start_date=START_DATE,
           catchup=False)

## Dummy tasks
begin = DummyOperator(task_id='begin', retries=1, dag=dag1)
end = DummyOperator(task_id='end', retries=1)

## Compute Engine tasks
stop_instance = StopInstanceOperator(
    project=PROJECT, zone=ZONE, instance=INSTANCE, task_id='stop_instance')
snapshot_disk = SnapshotDiskOperator(
    project=PROJECT, zone=ZONE, instance=INSTANCE,
    disk=DISK, task_id='snapshot_disk')
start_instance = StartInstanceOperator(
    project=PROJECT, zone=ZONE, instance=INSTANCE, task_id='start_instance')

# [START wait_tasks]
## Wait tasks
wait_for_stop = OperationStatusSensor(
    project=PROJECT, zone=ZONE, instance=INSTANCE,
    prior_task_id='stop_instance', poke_interval=15, task_id='wait_for_stop')
wait_for_snapshot = OperationStatusSensor(
    project=PROJECT, zone=ZONE, instance=INSTANCE,
    prior_task_id='snapshot_disk', poke_interval=10,
    task_id='wait_for_snapshot')
wait_for_start = OperationStatusSensor(
    project=PROJECT, zone=ZONE, instance=INSTANCE,
    prior_task_id='start_instance', poke_interval=5, task_id='wait_for_start')
# [END wait_tasks]

# Airflow DAG definition
begin >> stop_instance >> wait_for_stop >> snapshot_disk >> wait_for_snapshot \
        >> start_instance >> wait_for_start >> end
