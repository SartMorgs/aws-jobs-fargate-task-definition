import boto3
import os

from zipfile import ZipFile, ZIP_DEFLATED

class ZipFileGenerator:
	def __init__(self):
		self.s3_source_bucket = os.environ.get('S3_SOURCE_BUCKET')
		self.s3_source_folder = os.environ.get('S3_SOURCE_FOLDER')

		self.s3_target_bucket = os.environ.get('S3_TARGET_BUCKET')
		self.s3_target_folder = os.environ.get('S3_TARGET_FOLDER')

		self.zip_filename = os.environ.get('ZIP_FILENAME')

		self.region_name = os.environ.get('REGION_NAME', 'us-east-1')
		self.s3_resource = boto3.resource('s3', region_name=self.region_name)

	def get_file_from_s3(self, s3_file_path, local_filename):
		self.s3_resource.Bucket(self.s3_source_bucket).download_file(
			Key=s3_file_path,
			Filename=local_filename
		)

		print(f'Downloaded file {local_filename} from s3://{self.s3_source_bucket}/object_name')

		return local_filename

	def download_files_from_folder(self):

		paginator = self.s3_resource.meta.client.get_paginator('list_objects_v2')
		result = paginator.paginate(
			Bucket=self.s3_source_bucket,
			Prefix=self.s3_source_folder
		)

		s3_files = []
		local_files = []

		for page in result:
			if 'Contents' in page:
				for key in page[ "Contents" ]:
					s3_files.append(str(key[ "Key" ]))

		for s3_file in s3_files:
			folder = f'{self.s3_source_folder}/'
			local_filename = s3_file.replace(folder, '')
			if local_filename != '':
				local_files.append(self.get_file_from_s3(s3_file, local_filename))

		return local_files

	def generate_zip_file(self, local_files_list):
		zipfilename = f'{self.zip_filename}.zip'
		zipObj = ZipFile(zipfilename, 'w')

		for local_file in local_files_list:
			zipObj.write(local_file, compress_type=ZIP_DEFLATED)

		zipObj.close()

		print(f'Generated zip file sucessfully!')

	def delete_files_from_folder(self, local_files_list):
		for local_file in local_files_list:
			os.remove(local_file)

		print(f'All temp files was deleted!')

	def upload_zip_file(self):
		object_name =  f'{self.s3_target_folder}/{self.zip_filename}.zip'
		filename = f'{self.zip_filename}.zip'
		self.s3_resource.meta.client.upload_file(
			Bucket=self.s3_target_bucket,
			Key=object_name,
			Filename=filename
		)
		print(f'Uploaded file s3://{self.s3_target_bucket}/{object_name}')

if __name__ == "__main__":
	zip_file_generator = ZipFileGenerator()
	local_files = zip_file_generator.download_files_from_folder()
	zip_file_generator.generate_zip_file(local_files)
	zip_file_generator.delete_files_from_folder(local_files)
	zip_file_generator.upload_zip_file()