from minio import Minio
from minio.error import S3Error
from io import BytesIO


bucket_name = "my-bucket"
file_path = "/home/dmytro/Desktop/ОНМД/Курсова/image-encode-api/docker_utils/hello.txt"
object_name = "hello2.txt"

class MinioUtils:
    def __init__(self):
        client = Minio(
            "localhost:9000",
            access_key="minioadmin333",
            secret_key="minioadmin333",
            secure=False  # Set to True if using HTTPS
        )
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")
        self.client = client


    def createTestFile(self):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f"✅ '{file_path}' uploaded to bucket '{bucket_name}' as '{object_name}'.")
        except S3Error as err:
            print(f"❌ Upload failed: {err}")

    def getFile(self,def_object_name):
        try:
            object = self.client.get_object(bucket_name, def_object_name)
            return object
        except S3Error as err:
            print(f"❌ GET failed: {err}")

    def generate_signed_url(self,def_object_name):
        try:
            object = self.client.get_presigned_url('GET',bucket_name, def_object_name)
            return object
        except S3Error as err:
            print('smth');
    def addFragments(self, def_object_name, buffer):
        print(f"'{bucket_name}''{def_object_name}',{buffer.__sizeof__()}")
        # print(f"'{bucket_name}''{def_object_name}',{type(buffer)}")
        # print(f"'{bucket_name}''{def_object_name}',{buffer.tell()}")
        # buffer2 = BytesIO(buffer)
        # current_pos =buffer.seek(0, 2)
        size = buffer.getbuffer().nbytes
        # print(size, buffer.__sizeof__())
        # buffer.seek(0)

        try:
            self.client.put_object(bucket_name, def_object_name, buffer, size)
            print(f"✅ '{def_object_name}' uploaded to bucket '{bucket_name}''.")
        except S3Error as err:
            print(f"❌ Upload failed: {err}")



