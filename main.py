import io
import zipfile
import boto3
import tempfile

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    bucket_name = 'edubrandao-dados'
    snapshot_prefix_path = 'brandao'

    files = []

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=snapshot_prefix_path)

    for obj in response.get('Contents', []):
        response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        content_stream = response['Body']

        files.append({'name': obj['Key'], 'content': content_stream.read()})

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        with zipfile.ZipFile(temp_file, 'w') as zipf:
            for file in files:
                zipf.writestr(file['name'], file['content'])

    s3.upload_file(temp_file.name, bucket_name, f'{snapshot_prefix_path}/snapshotscompactados.zip')

    return {
        'statusCode': 200,
        'body': 'Arquivo ZIP criado e carregado com sucesso no S3.'
    }
