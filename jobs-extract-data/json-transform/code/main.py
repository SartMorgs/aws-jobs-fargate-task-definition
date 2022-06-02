import boto3
import os
import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class TransformJsonData:
    def __init__(self):
        self.schema_file_bucket = os.environ.get('SCHEMA_FILE_BUCKET')
        self.schema_file_folder = os.environ.get('SCHEMA_FILE_FOLDER')
        self.schema_file_filename = os.environ.get('SCHEMA_FILE_FILENAME')

        self.source_bucket = os.environ.get('SOURCE_BUCKET')
        self.source_folder = os.environ.get('SOURCE_FOLDER')
        self.source_json_filename = os.environ.get('SOURCE_JSON_FILENAME')

        self.target_bucket = os.environ.get('TARGET_BUCKET')
        self.target_folder = os.environ.get('TARGET_FOLDER')
        self.target_json_filename = os.environ.get('TARGET_JSON_FILENAME')

        self.region_name = 'us-east-1'
        self.s3_resource = boto3.resource('s3', region_name=self.region_name)

        self.local_yaml_filename = 'schema.yaml'
        self.local_json_source_filename = 'source_file.json'
        self.local_json_target_filename = 'target_file.json'

        self.headers_columns = []
        self.explode_columns = []
        self.inside_fields_columns = []

    def download_file(self, s3_bucket, s3_folder, s3_filename, local_filename):
        f = open(local_filename, 'a')
        f.write('')
        f.close()

        object_name = f'{s3_folder}/{s3_filename}'
        try:
            self.s3_resource.Bucket(s3_bucket).download_file(
                Key=object_name, 
                Filename=local_filename
            )
        except Exception as e:
            print(str(e))
            message_log = f'File object with incorrect filename format = \n{object_name}'
            print(message_log)

    def get_list_from_schema(self):
        file = open(self.local_yaml_filename)
        schema = yaml.safe_load(file)
        self.headers_columns = schema['schema']['headers']
        self.explode_columns = schema['schema']['explode']
        self.inside_fields_columns = schema['schema']['inside_fields']

    def start_session(self):
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL to fix json structure") \
            .enableHiveSupport() \
            .getOrCreate()
        print('Started pyspark session')
        return spark

    def read_dataframe(self, spark):
        df = spark.read.option('multiLine','true').json(self.local_json_source_filename)
        print(f'Total rows in json: {df.count()}')
        return df

    def header_columns_filter(self, df):
        AllDataDF = df.select(*self.headers_columns)
        df.unpersist()
        print('Header filters made successfully')
        print(f'Total rows in json: {AllDataDF.count()}')
        return AllDataDF

    def explode_header_columns(self, df):
        header_field = self.headers_columns[0]
        header_field_alias = f'{header_field}_2'

        for column_to_explode in self.explode_columns:
            column_prefix_alias = f'{column_to_explode}_'
            ColumnToExplodeDF = df.select(F.col(header_field), F.explode_outer(column_to_explode)).select(F.col(header_field).alias(header_field_alias),'col.*').drop(*['col'])
            select_list = [F.col(col_name).alias(column_prefix_alias + col_name) for col_name in ColumnToExplodeDF.columns]
            ColumnToExplodeDF = ColumnToExplodeDF.select(*select_list)
            header_field_alias = column_prefix_alias + header_field_alias

            partition_column = f'{column_to_explode}_{ColumnToExplodeDF.columns[0]}'
            
            df = df.join(ColumnToExplodeDF, df[header_field] == ColumnToExplodeDF[header_field_alias], 'full').drop(header_field_alias)
            ColumnToExplodeDF.unpersist()
        print('Explode columns made successfully')
        print(f'Total rows in json: {df.count()}')
        return df

    def expand_header_columns(self, df):
        headers_columns_to_expand = list(set(self.headers_columns) - set(self.explode_columns))
        for header_column in headers_columns_to_expand:
            column_to_expand_filter_exp = f'{header_column}.*'
            column_prefix_alias = f'{header_column}_'
            ColumnToExpandDF = df.select(column_to_expand_filter_exp)
            select_list = [F.col(col_name).alias(column_prefix_alias + col_name)  for col_name in ColumnToExpandDF.columns]
            
            partition_column = f'{header_column}_{ColumnToExpandDF.columns[0]}'
            ColumnToExpandDF = ColumnToExpandDF.select(*select_list)

            df = df.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
            ColumnToExpandDF = ColumnToExpandDF.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
            df = df.join(ColumnToExpandDF, on=['row_index']).drop('row_index').drop(header_column)
            ColumnToExpandDF.unpersist()
        print('Expand header columns made successfully')
        print(f'Total rows in json: {df.count()}')
        return df

    def expand_internal_columns(self, df):
        for element_to_expand in self.inside_fields_columns:
            columns_to_expand = self.inside_fields_columns[element_to_expand]
            for column_to_expand in columns_to_expand:
                column_to_expand_ = f'{element_to_expand}_{column_to_expand}'
                column_to_expand_filter_exp = f'{element_to_expand}_{column_to_expand}.*'
                column_prefix_alias = f'{element_to_expand}_{column_to_expand}_'
                ColumnToExpandDF = df.select(column_to_expand_filter_exp)
                select_list = [F.col(col_name).alias(column_prefix_alias + col_name)  for col_name in ColumnToExpandDF.columns]
                
                partition_column = f'{element_to_expand}_{column_to_expand}_{ColumnToExpandDF.columns[0]}'
                ColumnToExpandDF = ColumnToExpandDF.select(*select_list)

                df = df.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                ColumnToExpandDF = ColumnToExpandDF.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
                df = df.join(ColumnToExpandDF, on=['row_index']).drop('row_index').drop(column_to_expand)
                ColumnToExpandDF.unpersist()
        print('Expand internal columns made successfully')
        print(f'Total rows in json: {df.count()}')
        return df

    def save_target_json(self, df):
        df = df.select(sorted(df.columns))
        pandasDF2 = df.toPandas()
        json_data = pandasDF2.to_json(self.local_json_target_filename, orient='split')
        print('Target json generated successfully')

    def upload_json_to_s3(self):
        s3_object_name = f'{self.target_folder}/{self.target_json_filename}'
        self.s3_resource.meta.client.upload_file(self.local_json_target_filename, self.target_bucket, s3_object_name)
        print('Target json was uploades on s3 successfully')

if __name__ == '__main__':
    transform_json_data = TransformJsonData()
    transform_json_data.download_file(
        transform_json_data.schema_file_bucket, 
        transform_json_data.schema_file_folder, 
        transform_json_data.schema_file_filename,
        transform_json_data.local_yaml_filename
    )
    transform_json_data.get_list_from_schema()
    spark = transform_json_data.start_session()
    transform_json_data.download_file(
        transform_json_data.source_bucket,
        transform_json_data.source_folder,
        transform_json_data.source_json_filename,
        transform_json_data.local_json_source_filename
    )
    df = transform_json_data.read_dataframe(spark)
    df = transform_json_data.header_columns_filter(df)
    df = transform_json_data.explode_header_columns(df)
    df = transform_json_data.expand_header_columns(df)
    df = transform_json_data.expand_internal_columns(df)
    transform_json_data.save_target_json(df)
    transform_json_data.upload_json_to_s3()