import json
import os
import boto3
import time

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date, timedelta
from jinjasql import JinjaSql

class ExtractEventsFromGA():
	def __init__(self):
		self.start_date = os.environ.get('START_DATE')
		self.end_date = os.environ.get('END_DATE')

		self.tmp_table_bucket_name = os.environ.get('TEMP_TABLE_BUCKET')
		self.main_table_bucket_name = os.environ.get('MAIN_TABLE_BUCKET')
		self.main_table_folder = os.environ.get('MAIN_TABLE_FOLDER')

		self.artifacts_bucket_name = os.environ.get('ARTIFACTS_BUCKET_NAME')
		self.artifacts_query_folder = os.environ.get('ARTIFACTS_FOLDER_NAME')
		self.tmp_table_query_filename = os.environ.get('TMP_TABLE_QUERY_FILENAME')
		self.load_main_table_query_filename = os.environ.get('LOAD_MAIN_TABLE_QUERY_FILENAME')

		self.source_database_name = os.environ.get('SOURCE_DATABASE_NAME')
		self.s3_output_querys = os.environ.get('S3_OUTPUT_QUERY_PATH')

		self.region_name = 'us-east-1'
		self.athena_client = boto3.client('athena', region_name=self.region_name)
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)

		self.dimensions = os.environ.get('DIMENSIONS_LIST')
		self.metrics = os.environ.get('METRICS_LIST')

		self.view_id = os.environ.get('VIEW_ID')
		self.scopes = ['https://www.googleapis.com/auth/analytics.readonly']
		self.key_file_location = 'client_secrets.json'

		self.sql_to_execute = ''
		self.query_params = {}

	def delete_data_main_table(self, date):
		year = str(date.year).zfill(4)
		month = str(date.month).zfill(2)
		day = str(date.day).zfill(2)
		bucket_key = f'{self.main_table_folder}/year={year}/month={month}/day={day}'

		try:
			objects_to_delete = self.s3_resource.meta.client.list_objects(Bucket=self.main_table_bucket_name, Prefix=bucket_key)
			delete_keys = {'Objects': []}
			delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
			self.s3_resource.meta.client.delete_objects(Bucket=self.main_table_bucket_name, Delete=delete_keys)
			print(f'{datetime.now()} - Deleted data from s3://{self.main_table_bucket_name}/{bucket_key}')
		except Exception as e:
			print(str(e))

	def delete_data_tmp_table(self, date):
		bucket_key =  date

		try:
			objects_to_delete = self.s3_resource.meta.client.list_objects(Bucket=self.tmp_table_bucket_name, Prefix=bucket_key)
			delete_keys = {'Objects': []}
			delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
			self.s3_resource.meta.client.delete_objects(Bucket=self.tmp_table_bucket_name, Delete=delete_keys)
			print(f'{datetime.now()} - Deleted data from s3://{self.tmp_table_bucket_name}/{bucket_key}')
		except Exception as e:
			print(str(e))

	def download_query_from_s3(self, bucket, folder, filename):
		object_name = f'{folder}/{filename}'
		print(object_name)
		self.s3_resource.Bucket(bucket).download_file(
			Key=object_name, 
			Filename='query.sql'
		)

	def execute_query_athena(self, table_name):
		#try:
		with open('query.sql') as f:
			query_with_params = f.read()

			j = JinjaSql(param_style='pyformat')
			query, bind_params = j.prepare_query(query_with_params, self.query_params)
			self.sql_to_execute = (query % bind_params)

		response = self.athena_client.start_query_execution(
			QueryString = (self.sql_to_execute),
			QueryExecutionContext={
				'Database': self.source_database_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_querys + '/' + self.source_database_name,
			}
		)
		print(f'{datetime.now()} - Execution ID: ' + response['QueryExecutionId'])
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			#print(f'Query status: {query_status}')
			#print('...')
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				raise Exception('Athena query with the string \n"{}"\n failed or was cancelled'.format(self.sql_to_execute))
			time.sleep(2)
		print(f'{datetime.now()} - Query for {table_name} finished.')

	def initialize_analyticsreporting(self):
		credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_file_location, self.scopes)
		analytics = build('analyticsreporting', 'v4', credentials=credentials)
		return analytics

	def get_report(self, analytics, date, **kwargs):
		pageToken = kwargs.get('pageToken', None)

		if pageToken:
			return analytics.reports().batchGet(
				body={
					'reportRequests': [
						{
							'viewId': self.view_id,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'metrics': [
								{'expression': 'ga:totalEvents'},
								{'expression': 'ga:uniqueEvents'},
								{'expression': 'ga:eventValue'},
								{'expression': 'ga:avgEventValue'}
							],
							'dimensions': [
								{'name': 'ga:eventCategory'},
								{'name': 'ga:eventAction'}
							],
							'pageSize': '10000',
							'pageToken': str(pageToken)
						}]
					}
				).execute()
		else:
			return analytics.reports().batchGet(
				body={
					'reportRequests': [
						{
							'viewId': self.view_id,
							'dateRanges': [{'startDate': date, 'endDate': date}],
							'metrics': [
								{'expression': 'ga:totalEvents'},
								{'expression': 'ga:uniqueEvents'},
								{'expression': 'ga:eventValue'},
								{'expression': 'ga:avgEventValue'}
							],
							'dimensions': [
								{'name': 'ga:eventCategory'},
								{'name': 'ga:eventAction'}
							],
							'pageSize': '10000'
						}]
					}
				).execute()

	def extract_events_from_ga(self, date):
		analytics = self.initialize_analyticsreporting()

		response = self.get_report(analytics, date)

		while True:
			report = response.get('reports', [])
			try:
				next_token = report[0]['nextPageToken']
				print(f'{datetime.now()} - The next token is {next_token} and there have more events to get')
			except:
				print(f'{datetime.now()} - The next token not found, it will end soon')
				next_token = None

			if next_token:
				filename = f'data-{date}-{next_token}.json'
				with open(filename, 'w') as f:
					json.dump(report, f)
				response = self.get_report(analytics, date, pageToken=next_token)
			else:
				filename = f'data-{date}-last.json'
				with open(filename, 'w') as f:
					json.dump(report, f)
				key_name = f'{date}/{filename}'
				self.s3_resource.Bucket(self.tmp_table_bucket_name).upload_file(filename, key_name)
				os.remove(filename)
				break

			key_name = f'{date}/{filename}'
			self.s3_resource.Bucket(self.tmp_table_bucket_name).upload_file(filename, key_name)
			os.remove(filename)

	def load_ganalytics_table(self):
		start_date_ = datetime.strptime(self.start_date, '%Y-%m-%d')
		end_date_ = datetime.strptime(self.end_date, '%Y-%m-%d')

		print(f'Start date: {self.start_date} | End date: {self.end_date} | Days: {(end_date_ - start_date_).days}')

		for count in range((end_date_ - start_date_).days + 1):
			ref_date = start_date_ + timedelta(count)
			string_ref_data = ref_date.strftime('%Y-%m-%d')

			print(f'{datetime.now()} - Reference date: {string_ref_data}')

			self.delete_data_main_table(ref_date)

			self.extract_events_from_ga(string_ref_data)

			ref_data_for_table = string_ref_data.replace('-', '_')
			self.query_params['tmp_table_name'] = f'ga_events_{ref_data_for_table}'
			self.query_params['tmp_table_local'] = f's3://{self.tmp_table_bucket_name}/{string_ref_data}'

			folder = f'{self.artifacts_query_folder}'
			self.download_query_from_s3(self.artifacts_bucket_name, folder, self.tmp_table_query_filename)
			self.execute_query_athena(self.query_params['tmp_table_name'])

			self.query_params['year'] = str(ref_date.year).zfill(4)
			self.query_params['month'] = str(ref_date.month).zfill(2)
			self.query_params['day'] = str(ref_date.day).zfill(2)
			folder = f'{self.artifacts_query_folder}'
			self.download_query_from_s3(self.artifacts_bucket_name, folder, self.load_main_table_query_filename)
			self.execute_query_athena('ganalytics_consent')

			self.delete_data_tmp_table(string_ref_data)

			with open('query.sql', 'w') as query_file:
				query_drop_tmp_table = f'drop table ga_events_{ref_data_for_table}'
				query_file.truncate(0)
				query_file.write(query_drop_tmp_table)
			self.execute_query_athena(self.query_params['tmp_table_name'])


if __name__ == "__main__":
	extract_events_from_ga = ExtractEventsFromGA()
	extract_events_from_ga.load_ganalytics_table()
