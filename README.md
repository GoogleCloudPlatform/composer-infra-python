# Infrastructure Automation with Cloud Composer

This solution tutorial demonstrates a method to automate cloud infrastructure using [Cloud Composer](https://cloud.google.com/composer/).
As a practical example we have chosen to show how to schedule automated backups of  [Compute Engine](https://cloud.google.com/compute/) virtual machine instances.

Please refer to the article for the steps to run the code:
[SOLUTION URL]

## Contents of this repository

Cloud Composer [operators](https://airflow.apache.org/concepts.html#operators) and [DAG](https://airflow.apache.org/concepts.html#dags) to schedule automated backups:

* `no_sensor`: without using an [Aiflow sensor](https://airflow.apache.org/code.html#basesensoroperator)
* `sensor`: using an Airflow sensor. 

