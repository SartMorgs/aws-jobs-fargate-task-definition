import boto3
import psycopg2
import os

class DumpRdsTable:
	def __init__(self):
		self.bucket_name = os.environ.get('BUCKET')
		self.folder_name = os.environ.get('FOLDER')
		self.postgres_table = os.environ.get('POSTGRES_TABLE')
		self.postgres_host = os.environ.get('POSTGRES_HOST')
		self.postgres_port = os.environ.get('POSTGRES_PORT')
		self.postgres_database = os.environ.get('POSTGRES_DB')
		self.postgres_user = os.environ.get('POSTGRES_USER')
		self.postgres_password = os.environ.get('POSTGRES_PASSWORD')
		self.postgres_query = os.environ.get('DUMP_QUERY')

		self.file_name = self.postgres_table + '.csv'
		self.file_path = f'{self.folder_name}/{self.file_name}'

		self.s3_resource = boto3.resource('s3', region_name='us-east-1')

		self.postgres_connection = psycopg2.connect(
			host=self.postgres_host,
			port=self.postgres_port,
			database=self.postgres_database,
			user=self.postgres_user,
			password=self.postgres_password
		)

		self.postgres_cursor = self.postgres_connection.cursor()

		print(f'Postgres connection made successfully!\n')

	def dump_table(self):
		psql = f'SELECT * from aws_s3.query_export_to_s3(\'{self.postgres_query}\', aws_commons.create_s3_uri(\'{self.bucket_name}\', \'{self.file_path}\', \'us-east-1\'));'

		self.postgres_cursor.execute(psql)
		self.postgres_connection.commit()

		print(f'Table {self.postgres_table} exported successfully to s3://{self.bucket_name}/{self.file_path}\n')

if __name__ == "__main__":
	dump_rds_table = DumpRdsTable()
	dump_rds_table.dump_table()
