import psycopg2
import yaml
import time
import boto3
import io
import sys
import os

from jinjasql import JinjaSql
from datetime import datetime, date, timedelta
from smart_open import smart_open

class CopyFromS3ToPostgres():

	def __init__(self):

		self.postgres_host = os.environ.get('POSTGRES_HOST')
		self.postgres_port = os.environ.get('POSTGRES_PORT')
		self.postgres_database = os.environ.get('POSTGRES_DATABASE')
		self.postgres_user = os.environ.get('POSTGRES_USER')
		self.postgres_password = os.environ.get('POSTGRES_PASSWORD')
		self.postgres_connection = None
		self.sql_to_execute = ''

		self.table_configuration = {}
		self.query_params = {}
		self.output_info_file = {}
		self.data_count = 0
		self.data_count_expected = 0

		self.s3_artifacts_bucket = os.environ.get('S3_ARTIFACTS_BUCKET')
		self.s3_query_folder = os.environ.get('S3_QUERY_FOLDER')
		self.s3_query_filename = os.environ.get('S3_QUERY_FILENAME')

		self.source_database_name = os.environ.get('SOURCE_DATABASE_NAME')
		self.s3_output_querys = os.environ.get('S3_OUTPUT_QUERY_PATH')
		self.s3_reprocessing_logs_folder = os.environ.get('S3_REPROCESSING_LOGS_FOLDER')
		self.log_filename = os.environ.get('LOG_FILENAME')

		self.region_name = 'us-east-1'
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)
		self.athena_client = boto3.client('athena', region_name=self.region_name)
		self.s3_client = boto3.client('s3', region_name=self.region_name)

		self.table_configuration["schema"] = os.environ.get('TABLE_SCHEMA')
		self.table_configuration["table_name"] = os.environ.get('TABLE_NAME')
		self.table_configuration["data_field"] = os.environ.get('TABLE_DATA_FIELD')
		self.table_configuration["start_date"] = os.environ.get('TABLE_START_DATE')
		self.table_configuration["end_date"] = os.environ.get('TABLE_END_DATE')
		self.table_configuration["days_to_reprocessing"] = (
			datetime.strptime(
				self.table_configuration["start_date"], '%d-%m-%Y'
			).date() - datetime.strptime(
				self.table_configuration["end_date"], '%d-%m-%Y'
			).date()
		).days

	def reset_data_count(self):
		self.data_count = 0
		self.data_count_expected = 0

	def create_postgres_conection(self):
		try:
			self.postgres_connection = psycopg2.connect(
				host=self.postgres_host, 
				port=self.postgres_port,
				database=self.postgres_database, 
				user=self.postgres_user,
				password=self.postgres_password
			)

			print(f'{datetime.now()} - Postgres connection made successfully!\n')
		except Exception as e:
			self.add_message_log(str(e))

	def download_query_from_athena(self):
		object_name = f'{self.s3_query_folder}/{self.s3_query_filename}'
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

	def insert_parameters_into_query(self):
		with open('query.sql') as f:
			query_with_params = f.read()

			j = JinjaSql(param_style='pyformat')
			query, bind_params = j.prepare_query(query_with_params, self.query_params)
			self.sql_to_execute = (query % bind_params)

	def execute_query_athena(self):
		self.insert_parameters_into_query()

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.source_database_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.source_database_name,
			}
		)
		self.table_configuration["query_response_filename"] = response['QueryExecutionId']
		
		message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Execution ID = {self.table_configuration["query_response_filename"]}'
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
		print(f'{datetime.now()} - Query for {self.table_configuration["table_name"]} finished.')

	def delete_data_from_table(self, field, data):
		date_low = datetime.strptime(self.table_configuration["start_date"], '%d-%m-%Y').date()
		date_high = datetime.strptime(self.table_configuration["end_date"], '%d-%m-%Y').date()

		sql_delete = f'delete from \
		{self.table_configuration["schema"]}.{self.table_configuration["table_name"]} \
		where {field} = \'{data}\';'

		try:
			cur = self.postgres_connection.cursor()

			cur.execute(sql_delete)

			cur.close()
			self.postgres_connection.commit()

			message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Deleted data from table {self.table_configuration["schema"]}.{self.table_configuration["table_name"]}'
			self.add_message_log(message_log)
		except Exception as e:
			self.add_message_log(str(e))

	def copy_data_to_postgres(self):
		sql_copy = f'copy {self.table_configuration["schema"]}.{self.table_configuration["table_name"]} ' + \
		'''FROM STDIN WITH CSV HEADER NULL AS '';'''

		bucket_url = f'{self.s3_output_querys}/{self.source_database_name}/{self.table_configuration["query_response_filename"]}.csv'

		try:
			cur = self.postgres_connection.cursor()

			f = smart_open(bucket_url, 'rb', encoding='utf-8')
			cur.copy_expert(sql_copy, f)

			self.postgres_connection.commit()
			cur.close()

			message_log = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} - Copied {self.s3_output_querys} to postgres table {self.table_configuration["schema"]}.{self.table_configuration["table_name"]}'
			self.add_message_log(message_log)
		except Exception as e:
			self.add_message_log(str(e))

	def create_output_info_dict(self):
		self.output_info_file[self.table_configuration["table_name"]] = {}
		self.output_info_file[self.table_configuration["table_name"]]['table_name'] = self.table_configuration["table_name"]
		self.output_info_file[self.table_configuration["table_name"]]['execution_date'] = date.today()
		self.output_info_file[self.table_configuration["table_name"]]['logs'] = {}
		self.output_info_file[self.table_configuration["table_name"]]['logs']['error'] = []

	def put_error_message_into_output_info_dict(self, msg):
		self.output_info_file[self.table_configuration["table_name"]]['logs']['error'].append(str(msg))

	def increase_output_info_dict(self, days_to_reprocessing):
		reference_date = (date.today() + timedelta(days=days_to_reprocessing)).strftime('%Y-%m-%d')
		self.output_info_file[self.table_configuration["table_name"]]["reference_date"] = reference_date
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

		self.s3_client.put_object(Bucket=self.s3_artifacts_bucket, Key=s3_filename)
		self.s3_resource.Bucket(self.s3_artifacts_bucket).upload_file(Filename=filename, Key=s3_filename)

		s3_file_path = f'{self.s3_artifacts_bucket}/{self.s3_reprocessing_logs_folder}'
		print(f'Uploaded output log file {filename} in {s3_file_path}\n')

	def reprocessing_table(self):
		end_date = datetime.strptime(self.table_configuration["end_date"], '%d-%m-%Y').date()
		start_count = 0

		while (start_count >= self.table_configuration["days_to_reprocessing"]):
			reference_date = end_date + timedelta(start_count)
			date_diff = (reference_date - date.today()).days

			self.query_params["days_gone"] = date_diff
			print(f'{datetime.now()} - days gone: {self.query_params["days_gone"]}')

			try:
				self.increase_output_info_dict(date_diff)
				message_log = f'{datetime.now()} - reference data = {reference_date} | days gone = {date_diff} | start_count = {start_count} | days_to_reprocessing = {self.table_configuration["days_to_reprocessing"]}'
				self.add_message_log(message_log)
				self.delete_data_from_table(self.table_configuration["data_field"], reference_date)
				self.execute_query_athena()
				self.copy_data_to_postgres()
			except Exception as e:
				self.add_message_log(str(e))

			start_count -= 1

		self.input_out_file()

	def run_reprocessing_for_all_tables(self):
		self.create_postgres_conection()
		self.reprocessing_table()

if __name__ == "__main__":
	copy_from_s3_to_postgres = CopyFromS3ToPostgres()
	copy_from_s3_to_postgres.create_out_file()
	copy_from_s3_to_postgres.create_output_info_dict()
	copy_from_s3_to_postgres.download_query_from_athena()
	copy_from_s3_to_postgres.run_reprocessing_for_all_tables()
	copy_from_s3_to_postgres.save_output_file_on_bucket()