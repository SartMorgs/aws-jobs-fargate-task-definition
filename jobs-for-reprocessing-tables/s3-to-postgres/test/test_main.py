import unittest
import psycopg2
import os
import shutil
import boto3
import time
import csv
from datetime import datetime, date
from smart_open import smart_open
from code.main import CopyFromS3ToPostgres

class TestCopyFromS3ToPostgres(unittest.TestCase):
	def setUp(self):
		os.environ['POSTGRES_HOST'] = '10.200.1.11'
		os.environ['POSTGRES_PORT'] = '5432'
		os.environ['POSTGRES_DATABASE'] = 'postgres'
		os.environ['POSTGRES_USER'] = 'analytics'
		os.environ['POSTGRES_PASSWORD'] = 'fw6J3pvn3xvnW0sSQQ4shhRnQjM6GkCw'

		os.environ['TABLE_START_DATE'] = '17-05-2022'
		os.environ['TABLE_END_DATE'] = '18-05-2022'
		self.days_gone = (datetime.strptime('2022-05-17', '%Y-%m-%d').date() - date.today()).days
		self.s3_resource = boto3.resource('s3', region_name='us-east-1')
		self.copy_from_s3_to_postgres = CopyFromS3ToPostgres()

	def test_reset_data_count(self):
		expected_data_count = 0
		expected_data_count_expected = 0

		self.copy_from_s3_to_postgres.reset_data_count()

		received_data_count = self.copy_from_s3_to_postgres.data_count
		received_data_count_expected = self.copy_from_s3_to_postgres.data_count_expected

		self.assertEqual(received_data_count, expected_data_count)
		self.assertEqual(received_data_count_expected, expected_data_count_expected)

	def test_create_out_file(self):
		expected_filename = 'out/test_table.yaml'
		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.create_out_file()
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

		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.create_output_info_dict()
		received_output_info_file = self.copy_from_s3_to_postgres.output_info_file

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

		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_postgres.put_error_message_into_output_info_dict('test error 01')
		received_output_info_file = self.copy_from_s3_to_postgres.output_info_file

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

		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_postgres.increase_output_info_dict(self.days_gone)
		received_output_info_file = self.copy_from_s3_to_postgres.output_info_file

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
		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_postgres.output_info_file['test_table']['reference_date'] = '2022-05-17'
		self.copy_from_s3_to_postgres.output_info_file['test_table']['logs']['2022-05-17'] = []
		self.copy_from_s3_to_postgres.add_message_log('test log message')
		received_output_info_file = self.copy_from_s3_to_postgres.output_info_file

		self.assertEqual(received_output_info_file, expected_output_info_file)

	def test_insert_parameters_into_query(self):
		expected_query_filename = open('test/files/test_table_after_date.sql')
		expected_query = expected_query_filename.read()

		shutil.copyfile('test/files/test_table_before_date.sql', 'query.sql')
		self.copy_from_s3_to_postgres.target_database_name = 'test_database'
		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.query_params["start_date"] = '\'2022-05-17\''
		self.copy_from_s3_to_postgres.insert_parameters_into_query()
		received_query = self.copy_from_s3_to_postgres.sql_to_execute

		self.assertEqual(received_query, expected_query)

'''
	def test_copy_data_to_postgres(self):
		expected_content_table_file = open('test/files/table_content_expected.csv')
		expected_content_table = expected_content_table_file.read()
		expected_content_table_file.close()

		postgres_connection = psycopg2.connect(
			host=self.copy_from_s3_to_postgres.postgres_host, 
			port=self.copy_from_s3_to_postgres.postgres_port,
			database=self.copy_from_s3_to_postgres.postgres_database, 
			user=self.copy_from_s3_to_postgres.postgres_user,
			password=self.copy_from_s3_to_postgres.postgres_password
		)

		try:
			sql_delete = f'delete from public.test_table;'
			cur = postgres_connection.cursor()
			cur.execute(sql_delete)
			cur.close()
			postgres_connection.commit()
		except:
			print('Não foi possível deletar!')

		#copy query_after content to query.sql used by function
		shutil.copyfile('test/files/test_table_before_days.sql', 'query.sql')

		#set test scenario
		sql_to_execute_file = open('query.sql')
		self.copy_from_s3_to_postgres.sql_to_execute = sql_to_execute_file.read()
		self.copy_from_s3_to_postgres.source_database_name = 'test_database'
		self.copy_from_s3_to_postgres.s3_output_querys = 's3://bi-dl-artifacts/test-files/table-reprocessing-jobs/s3-to-postgres/file-to-insert'
		self.copy_from_s3_to_postgres.table_configuration["schema"] = 'public'
		self.copy_from_s3_to_postgres.table_configuration["table_name"] = 'test_table'
		self.copy_from_s3_to_postgres.table_configuration["query_response_filename"] = '07939fe1-6fed-4149-a7d5-d5581c2e3f9e'
		self.copy_from_s3_to_postgres.query_params["days_gone"] = self.days_gone
		self.copy_from_s3_to_postgres.output_info_file = {
			'test_table': {
				'table_name': 'test_table',
				'execution_date': date.today(),
				'logs': {
					'error': []
				}
			} 
		}
		self.copy_from_s3_to_postgres.table_configuration['table_name'] = 'test_table'
		self.copy_from_s3_to_postgres.output_info_file['test_table']['reference_date'] = '2022-05-17'
		self.copy_from_s3_to_postgres.output_info_file['test_table']['logs']['2022-05-17'] = []

		#execute test
		self.copy_from_s3_to_postgres.create_postgres_conection()
		self.copy_from_s3_to_postgres.copy_data_to_postgres()

		#get result
		sql_to_assert = f'select * from public.test_table'
		cur = postgres_connection.cursor()
		cur.execute(sql_to_assert)
		with open('test/files/table_content_received.csv', 'w') as table_content_file:
			write = csv.writer(table_content_file)
			write.writerow(cur.fetchall())
		cur.close()

		received_content_table_file = open('test/files/table_content_received.csv')
		received_content_table = received_content_table_file.read()
		received_content_table_file.close()

		self.assertEqual(expected_content_table, received_content_table)
'''