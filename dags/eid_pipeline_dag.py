from airflow import DAG
from airflow.utils.dates import days_ago 
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
	DataprocCreateClusterOperator,
	DataprocSubmitJobOperator,
	DataprocDeleteClusterOperator
	)

PROJECT_ID="retail-proj-1"
REGION="us-central1"
CLUSTER_NAME="eid-temp-cluster"

CLUSTER_CONFIG={
	"master_config":{
		"num_instances":1,
		"machine_type_uri":"n1-standard-2",
	},
	"worker_config":{
		"num_instances":2,
		"machine_type_uri":"n1-standard-2",
	},
}

with DAG(
	dag_id="eid_traditional_cluster_pipeline",
	start_date=days_ago(1),
	schedule_interval=None,
	catchup=False,
) as dag:
	
	# 1. Wait for raw file:
	wait_for_file=GCSObjectExistenceSensor(
		task_id="wait_for_raw_file",
		bucket="insurance-28022026",
		object="raw-data/eid_healthcare_1gb.txt",
	)

	# 2. Create Dataproc Cluster:
	create_cluster=DataprocCreateClusterOperator(
		task_id="create_cluster",
		project_id=PROJECT_ID,
		cluster_name=CLUSTER_NAME,
		region=REGION,
		cluster_config=CLUSTER_CONFIG,
	)

	# 3. Raw to Parquet Job
	raw_to_parquet=DataprocSubmitJobOperator(
		task_id="raw_to_parquet",
		project_id=PROJECT_ID,
		region=REGION,
		job={
			"placement":{"cluster_name":CLUSTER_NAME},
			"pyspark_job":{
				"main_python_file_uri":"gs://insurance-28022026/scripts/raw_to_parquet.py"
			},
		},
	)

	# 4. Transform --> BigQuery Job
	gcsbronze_to_bq=DataprocSubmitJobOperator(
		task_id="gcsbronge_to_bq",
		project_id=PROJECT_ID,
		region=REGION,
		job={
			"placement":{"cluster_name":CLUSTER_NAME},
			"pyspark_job":{
				"main_python_file_uri":"gs://insurance-28022026/scripts/gcsbronze_to_bq.py"
			},
		},
	)

	# 5. Delete Cluster (Very Important)
	delete_cluster=DataprocDeleteClusterOperator(
		task_id="delete_cluster",
		project_id=PROJECT_ID,
		region=REGION,
		cluster_name=CLUSTER_NAME,
		trigger_rule="all_done", #delete even if job fails
	)

	wait_for_file >> create_cluster >> raw_to_parquet >> gcsbronze_to_bq >> delete_cluster

