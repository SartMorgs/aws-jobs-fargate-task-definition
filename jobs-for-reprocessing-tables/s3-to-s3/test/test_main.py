import unittest
import os
import shutil
import boto3
import time
from datetime import datetime, date
from code.main import CopyFromS3ToS3

class TestCopyFromS3ToS3(unittest.TestCase):
	def setUp(self):
		os.environ['TABLE_START_DATE'] = '17-05-2022'
		os.environ['TABLE_END_DATE'] = '18-05-2022'
		self.days_gone = (datetime.strptime('2022-05-17', '%Y-%m-%d').date() - date.today()).days
		self.s3_resource = boto3.resource('s3', region_name='us-east-1')
		self.copy_from_s3_to_s3 = CopyFromS3ToS3()


	def test_create_out_file(self):
		expected_filename = 'out/test_table.yaml'
		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.create_out_file()
		result_test_if_file_exists = os.path.exists(expected_filename)
		self.assertTrue(result_test_if_file_exists)


	def test_create_output_info_dict(self):
		expected_output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}

		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.create_output_info_dict()
		received_output_info_file = self.copy_from_s3_to_s3.output_info_file

		self.assertEqual(received_output_info_file, expected_output_info_file)


	def test_put_error_message_into_output_info_dict(self):
		expected_output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': ['test error 01']
				}
			} 
		}

		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_s3.put_error_message_into_output_info_dict('test error 01')
		received_output_info_file = self.copy_from_s3_to_s3.output_info_file

		self.assertEqual(received_output_info_file, expected_output_info_file)


	def test_increase_output_info_dict(self):
		expected_output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'reference_date': '2022-05-17',
				'logs': {
					'2022-05-17': [],
					'error': []
				}
			} 
		}

		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_s3.increase_output_info_dict(self.days_gone)
		received_output_info_file = self.copy_from_s3_to_s3.output_info_file

		self.assertEqual(received_output_info_file, expected_output_info_file)


	def test_add_message_log(self):
		expected_output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'reference_date': '2022-05-17',
				'logs': {
					'2022-05-17': ['test log message'],
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_s3.output_info_file['test_table']['reference_date'] = '2022-05-17'
		self.copy_from_s3_to_s3.output_info_file['test_table']['logs']['2022-05-17'] = []
		self.copy_from_s3_to_s3.add_message_log('test log message')
		received_output_info_file = self.copy_from_s3_to_s3.output_info_file

		self.assertEqual(received_output_info_file, expected_output_info_file)


	def test_insert_parameters_into_query(self):
		expected_query_filename = open('test/files/test_table_after_date.sql')
		expected_query = expected_query_filename.read()

		shutil.copyfile('test/files/test_table_before_date.sql', 'query.sql')
		self.copy_from_s3_to_s3.target_database_name = 'test_database'
		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.query_params["start_date"] = '\'2022-05-17\''
		self.copy_from_s3_to_s3.insert_parameters_into_query()
		received_query = self.copy_from_s3_to_s3.sql_to_execute

		self.assertEqual(received_query, expected_query)


	def test_insert_data_into_table(self):
		expected_content_table_file = open('test/files/table_content_expected.csv')
		expected_content_table = expected_content_table_file.read()
		expected_content_table_file.close()

		# Delete current date for 2022-05-17 if exists
		try:
			objects_to_delete = self.s3_resource.meta.client.list_objects(
				Bucket='bi-dl-artifacts', 
				Prefix='test-files/table-reprocessing-jobs/s3-to-s3/test-table/year=2022/month=05/day=17'
			)
			delete_keys = {'Objects': []}
			delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
			self.s3_resource.meta.client.delete_objects(Bucket='bi-dl-artifacts', Delete=delete_keys)
		except:
			print('Empty Table!')

		#copy query_after content to query.sql used by function
		shutil.copyfile('test/files/test_table_before_days.sql', 'query.sql')

		#set test scenario
		sql_to_execute_file = open('query.sql')
		self.copy_from_s3_to_s3.sql_to_execute = sql_to_execute_file.read()
		self.copy_from_s3_to_s3.source_database_name = 'test_database'
		self.copy_from_s3_to_s3.target_database_name = 'test_database'
		self.copy_from_s3_to_s3.s3_output_querys = 's3://bi-dl-artifacts/query-results'
		self.copy_from_s3_to_s3.s3_folder_table = 'test_table'
		self.copy_from_s3_to_s3.query_params["days_gone"] = self.days_gone
		self.copy_from_s3_to_s3.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_s3.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_s3.output_info_file['test_table']['reference_date'] = '2022-05-17'
		self.copy_from_s3_to_s3.output_info_file['test_table']['logs']['2022-05-17'] = []

		#execute test
		self.copy_from_s3_to_s3.insert_data_into_table()

		#get result
		sql_to_execute_file = open('test/files/test_load_table.sql')
		self.copy_from_s3_to_s3.sql_to_execute = sql_to_execute_file.read()
		response = self.copy_from_s3_to_s3.athena_client.start_query_execution(
			QueryString = (self.copy_from_s3_to_s3.sql_to_execute),
			QueryExecutionContext={
				'Database': self.copy_from_s3_to_s3.source_database_name
			},
			ResultConfiguration={
				'OutputLocation': self.copy_from_s3_to_s3.s3_output_querys + '/' + self.copy_from_s3_to_s3.s3_folder_table,
			}
		)
		result_query_filename = 'query-results/' + self.copy_from_s3_to_s3.s3_folder_table + '/' + response['QueryExecutionId'] + '.csv'
		print(result_query_filename)

		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.copy_from_s3_to_s3.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				print('Query for test failed!')
			time.sleep(2)

		self.s3_resource.Bucket('bi-dl-artifacts').download_file(
			Key=result_query_filename, 
			Filename='test/files/table_content_received.csv'
		)
		received_content_table_file = open('test/files/table_content_received.csv')
		received_content_table = received_content_table_file.read()
		received_content_table_file.close()

		self.assertEqual(expected_content_table, received_content_table)


if __name__ == "__main__":
	unittest.main()