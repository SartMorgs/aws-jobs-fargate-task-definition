import boto3
import yaml
import os
import time

from jinjasql import JinjaSql
from yaml.loader import SafeLoader
from datetime import datetime, date, timedelta

class CopyFromJsonToParquet():
	def __init__(self):
		self.artifacts_bucket_name = os.environ.get('ARTIFACTS_BUCKET')
		self.tmp_query_folder = os.environ.get('TMP_QUERY_FOLDER')
		self.tmp_query_filename = os.environ.get('TMP_QUERY_FILENAME')
		self.s3_output_querys = os.environ.get('OUTPUT_QUERY')

		self.tmp_table_db_name = os.environ.get('TMP_TABLE_DB_NAME')
		self.json_table_bucket = os.environ.get('JSON_TABLE_BUCKET')
		self.json_table_folder = os.environ.get('JSON_TABLE_FOLDER')

		self.load_query_folder = os.environ.get('TABLE_QUERY_FOLDER')
		self.load_query_filename = os.environ.get('TABLE_QUERY_FILENAME')

		self.table_bucket_name = os.environ.get('TABLE_BUCKET')
		self.table_folder_name = os.environ.get('TABLE_FOLDER')

		self.query_params = {}
		self.output_info_file = {}
		self.table_configuration = {}

		self.region_name = 'us-east-1'
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)
		self.athena_client = boto3.client('athena', region_name=self.region_name)
		self.s3_client = boto3.client('s3', region_name=self.region_name)

		self.s3_reprocessing_logs_folder = os.environ.get('S3_REPROCESSING_LOGS_FOLDER')
		self.log_filename = os.environ.get('LOG_FILENAME') 

		self.table_configuration["table_name"] = os.environ.get('TABLE_NAME') 
		self.table_configuration["start_date"] = os.environ.get('TABLE_START_DATE') 
		self.table_configuration["end_date"] = os.environ.get('TABLE_END_DATE') 
		self.table_configuration["days_to_reprocessing"] = (
			datetime.strptime(
				self.table_configuration["start_date"], '%d-%m-%Y'
			).date() - datetime.strptime(
				self.table_configuration["end_date"], '%d-%m-%Y'
			).date()
		).days

		self.table_configuration["partitioned"] = os.environ.get('BOOL_TABLE_PARTICIONED') 
		self.table_configuration["partition_list"] = os.environ.get('TABLE_PARTITION_LIST').strip('][').split(', ') 

	def download_query_from_s3(self, query_folder, query_filename):
		object_name = f'{query_folder}/{query_filename}'
		try:
			self.s3_resource.Bucket(self.s3_artifacts_bucket).download_file(
				Key=object_name, 
				Filename='query.sql'
			)
		except Exception as e:
			self.put_error_message_into_output_info_dict(str(e))
			message_log = f'Query file object with incorrect filename format = \n{object_name}'
			self.put_error_message_into_output_info_dict(message_log)

	def add_message_log(self, msg):
		ref_date = self.output_info_file[self.table_configuration["table_name"]]["reference_date"]
		print(msg)
		self.output_info_file[self.table_configuration["table_name"]]['logs'][ref_date].append(msg)

	def create_tmp_table(self, query_filename, tmp_table_name, tmp_table_location):
		with open(query_filename) as f:
			query_with_params = f.read()

			j = JinjaSql(param_style='pyformat')
			query, bind_params = j.prepare_query(query_with_params, self.query_params)
			self.sql_to_execute = (query % bind_params)

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.tmp_table_db_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.table_folder_name,
			}
		)
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Execution ID = ' + response['QueryExecutionId']
		self.add_message_log(message_log)
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				message_log = f'Athena query with the string \n{self.sql_to_execute}\n failed or was cancelled'
				self.add_message_log(message_log)
				failed_reason = self.athena_client.get_query_execution(response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
				self.add_message_log(failed_reason)
			time.sleep(2)
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Create tmp table {tmp_table_name} from {tmp_table_location} finished.'
		self.add_message_log(message_log)

	def create_output_info_dict(self):
		self.output_info_file[self.table_configuration["table_name"]] = {}
		self.output_info_file[self.table_configuration["table_name"]]['table_name'] = self.table_configuration["table_name"]
		self.output_info_file[self.table_configuration["table_name"]]['execution_date'] = date.today()
		self.output_info_file[self.table_configuration["table_name"]]['logs'] = {}
		self.output_info_file[self.table_configuration["table_name"]]['logs']['error'] = []

	def put_error_message_into_output_info_dict(self, msg):
		self.output_info_file[self.table_configuration["table_name"]]['logs']['error'].append(str(msg))

	def increase_output_info_dict(self, days_to_reprocessing, hour_reference):
		reference_date = (date.today() + timedelta(days=days_to_reprocessing)).strftime('%Y-%m-%d')
		reference_date_hour = f'reference_date {hour_reference}H'
		self.output_info_file[self.table_configuration["table_name"]]['logs'][reference_date] = []

	def create_out_file(self):
		filename = f'out/{self.table_configuration["table_name"]}.yaml'
		f = open(filename, "w")
		f.write('')
		f.close

	def input_out_file(self):
		filename = f'out/{self.table_configuration["table_name"]}.yaml'

		with open(filename, "w") as yaml_file:
			yaml.dump(self.output_info_file, yaml_file, default_flow_style=False)

	def save_output_file_on_bucket(self):
		filename = f'out/{self.table_configuration["table_name"]}.yaml'
		current_date = date.today()
		s3_filename = f'{self.s3_reprocessing_logs_folder}/{self.table_configuration["table_name"]}/{current_date}/{self.log_filename}.yaml'

		self.s3_client.put_object(Bucket=self.artifacts_bucket_name, Key=s3_filename)
		self.s3_resource.Bucket(self.artifacts_bucket_name).upload_file(Filename=filename, Key=s3_filename)

		s3_file_path = f'{self.artifacts_bucket_name}/{self.s3_reprocessing_logs_folder}'
		print(f'Uploaded output log file {filename} in {s3_file_path}\n')

	def delete_data_from_table(self, date, hour_partition, partition, partition_list_field):

		if partition:
			bucket_key = self.table_folder_name
			for item in partition_list_field:
				if 'year' in item:
					bucket_key = bucket_key + '/year=' + str(date.year).zfill(4)
				elif 'month' in item:
					bucket_key = bucket_key + '/month=' + str(date.month).zfill(2)
				elif 'day' in item:
					bucket_key = bucket_key + '/day=' + str(date.day).zfill(2)
				elif 'hour' in item:
					bucket_key = bucket_key + '/hour=' + str(hour_partition).zfill(2)
		else:
			bucket_key = self.table_folder_name

		try:
			objects_to_delete = self.s3_resource.meta.client.list_objects(Bucket=self.table_bucket_name, Prefix=bucket_key)
			delete_keys = {'Objects': []}
			delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
			self.s3_resource.meta.client.delete_objects(Bucket=self.table_bucket_name, Delete=delete_keys)
			message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Deleted data from s3://{self.table_bucket_name}/{bucket_key}'
			self.add_message_log(message_log)
		except Exception as e:
			self.add_message_log(str(e))

	def drop_tmp_table(self, tmp_table_name):
		self.sql_to_execute = f'drop table {self.tmp_table_db_name}.{tmp_table_name}'

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.tmp_table_db_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.table_folder_name,
			}
		)
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				message_log = f'Athena query with the string \n{self.sql_to_execute}\n failed or was cancelled'
				self.add_message_log(message_log)
				failed_reason = self.athena_client.get_query_execution(response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
				self.add_message_log(failed_reason)
			time.sleep(2)
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Dropped tmp table {tmp_table_name}.'
		self.add_message_log(message_log)


	def insert_parquet_table(self, query_filename, table_name, tmp_table_name):
		with open(query_filename) as f:
			query_with_params = f.read()

			j = JinjaSql(param_style='pyformat')
			query, bind_params = j.prepare_query(query_with_params, self.query_params)
			self.sql_to_execute = (query % bind_params)

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.tmp_table_db_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.table_folder_name,
			}
		)
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Execution ID = ' + response['QueryExecutionId']
		self.add_message_log(message_log)
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				message_log = f'Athena query with the string \n{self.sql_to_execute}\n failed or was cancelled'
				self.add_message_log(message_log)
				failed_reason = self.athena_client.get_query_execution(response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
				self.add_message_log(failed_reason)
			time.sleep(2)
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Load table {table_name} from {tmp_table_name} finished.'
		self.add_message_log(message_log)

	def reprocess_table(self):
		end_date = datetime.strptime(self.table_configuration["end_date"], '%d-%m-%Y').date()
		start_count = 0

		self.query_params['Table_Name'] = self.table_configuration["table_name"]

		while (start_count >= self.table_configuration["days_to_reprocessing"]):
			reference_date = end_date + timedelta(start_count)
			date_diff = (reference_date - date.today()).days

			self.query_params["days_gone"] = date_diff
			print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - days gone: {self.query_params["days_gone"]}')
			hour_count = 0
			while hour_count <= 23:
				self.query_params['Year'] = year = str(reference_date.year).zfill(4)
				self.query_params['Month'] = month = str(reference_date.month).zfill(2)
				self.query_params['Day'] = day = str(reference_date.day).zfill(2)
				self.query_params['Hour'] = hour = str(hour_count).zfill(2)

				self.query_params['Year'] 
				self.query_params['Tmp_Table_Name'] = f'tmp_recharge_{year}_{month}_{day}_{hour}'
				self.query_params['Tmp_Table_Location'] = f'\'s3://{json_to_parquet.json_table_bucket}/{json_to_parquet.json_table_folder}/{year}/{month}/{day}/{hour}/\''
				
				try:
					self.increase_output_info_dict(date_diff, hour)
					message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - reference data: {reference_date} | reference_hour: {hour_count}| days_to_reprocessing: {self.table_configuration["days_to_reprocessing"]}'
					self.add_message_log(message_log)
					self.delete_data_from_table(reference_date, hour_count, self.table_configuration["partitioned"], self.table_configuration["partition_list"])
					self.create_tmp_table(self.tmp_query_filename, self.query_params['Tmp_Table_Name'], self.query_params['Tmp_Table_Location'])
					self.insert_parquet_table(self.load_query_filename, self.query_params['Table_Name'], self.query_params['Tmp_Table_Name'])
					self.drop_tmp_table(self.query_params['Tmp_Table_Name'])
				except Exception as e:
					self.add_message_log(str(e))

				hour_count += 1

			start_count -= 1

		self.input_out_file()


if __name__ == "__main__":
	json_to_parquet = CopyFromJsonToParquet()
	json_to_parquet.create_out_file()
	json_to_parquet.create_output_info_dict()
	json_to_parquet.download_query_from_s3(
		json_to_parquet.tmp_query_folder,
		json_to_parquet.tmp_query_filename
	)
	json_to_parquet.download_query_from_s3(
		json_to_parquet.load_query_folder,
		json_to_parquet.load_query_filename
	)
	json_to_parquet.reprocess_table()
	json_to_parquet.save_output_file_on_bucket()