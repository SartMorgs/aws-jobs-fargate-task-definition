import boto3
import os
import csv

class CsvToTxtReport:
    def __init__(self):
        self.s3_bucket = os.environ.get('S3_BUCKET')
        self.s3_folder = os.environ.get('S3_FOLDER')
        self.s3_filename = os.environ.get('S3_FILENAME')

        self.region_name = os.environ.get('REGION_NAME')
        self.s3_resource = boto3.resource('s3', region_name=self.region_name)

        self.txt_separator = os.environ.get('FILE_SEPARATOR')

    def get_file_from_s3(self):
        object_name = f'{self.s3_folder}/{self.s3_filename}.csv'
        local_filename = 'file.csv'
        self.s3_resource.Bucket(self.s3_bucket).download_file(
            Key=object_name,
            Filename=local_filename
        )

        print(f'Downloaded file {local_filename} from s3://{self.s3_bucket}/object_name')

        return local_filename

    def convert_csv_to_txt(self, source_file):
        target_file = 'file.txt'
        with open(target_file, "w", encoding='utf-8-sig') as my_output_file:
            with open(source_file, "r", encoding='utf-8-sig', errors='backslashreplace') as my_input_file:
                [ my_output_file.write(self.txt_separator.join(row)+'\n') for row in csv.reader(l.replace('\0', '') for l in my_input_file) ]
            my_output_file.close()

        print(f'Converted csv file to txt sucessfully!')
        return target_file

    def put_file_into_s3(self, target_file):
        object_name = f'{self.s3_folder}/{self.s3_filename}.txt'
        self.s3_resource.meta.client.upload_file(
            Bucket=self.s3_bucket,
            Key=object_name,
            Filename=target_file
        )
        print(f'Uploaded file s3://{self.s3_bucket}/{object_name}')

if __name__ == "__main__":
    csv_to_txt = CsvToTxtReport()
    source_file = csv_to_txt.get_file_from_s3()
    target_file = csv_to_txt.convert_csv_to_txt(source_file)
    csv_to_txt.put_file_into_s3(target_file)