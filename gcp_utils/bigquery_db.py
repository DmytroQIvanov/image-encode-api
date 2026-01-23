from decorators import timing
import numpy as np
from annoy import AnnoyIndex
from google.cloud import bigquery
import os
import cv2
import pandas as pd
from typing import Optional, List

from docker_utils.minio_util import MinioUtils
from fragment import Fragment
# from .credentials import CREDENTIALS
from .bucket_utils import MinioBucketUtils
from .fragments_storage import FragmentsStorage


import psycopg2
from psycopg2 import sql
import psycopg2.extras  # додаткові можливості


# Fetch all results
# rows = cur.fetchall()



class BigQueryDB:
    # TABLE_CONFIG = bigquery.LoadJobConfig(
    #     schema=[
    #         bigquery.SchemaField("id", "INTEGER", "REQUIRED"),
    #         bigquery.SchemaField("image", "BYTES", "REQUIRED"),  # <== ключове
    #         bigquery.SchemaField("features", "BYTES", "REQUIRED")
    #     ]
    # )

    def __init__(self):
        super().__init__()
        # Завантажуємо облікові дані
        # Створюємо клієнта BigQuery
        # self.client = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id)
        self.tree = None
        # self.project_id = CREDENTIALS.project_id
        # self.dataset_id = os.environ['GCP_DATASET_ID']
        # self.table_id = os.environ['GCP_TABLE_ID']
        # self.target_table = self.client.get_table(f"{self.project_id}.{self.dataset_id}.{self.table_id}")
        self.storage = FragmentsStorage()
        self.label_generator = None
        self.bucket_storage = MinioBucketUtils()
        self.buffer_fragments_ids = []
        # ---POSTGRESQL---
        self.conn = psycopg2.connect(
            host="postgres",
            database="postgres",
            user="postgres",
            password="postgres22312321",
            port=5432,
            connect_timeout=10
        )
        self.cur = self.conn.cursor()
        # --- MINIO ---
        self.minio = MinioUtils()
        self._create_tables()
        self.prepare_fragments()

    def _create_tables(self):
        self.cur.execute('''
                    CREATE TABLE IF NOT EXISTS image(
                        id INTEGER PRIMARY KEY,
                        image BYTEA NOT NULL,
                        features BYTEA NOT NULL)
                    ''')
        self.conn.commit()

    def is_empty(self):
        return len(self.storage) == 0

    # ------------------
    # --- REFACTORED ---
    # ------------------
    @timing("Time preparing fragments")
    def prepare_fragments(self):

        # self.cur.execute(f"SELECT * FROM image where id={self.table_id}")
        # self.cur.execute("SELECT * FROM image WHERE id = %s", (self.table_id))
        # self.cur.execute("SELECT * FROM image WHERE id = %s", (self.table_id))
        self.cur.execute("SELECT * FROM image")

        image = self.cur.fetchall()


        # query = f"-- SELECT * FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`"
        # features_df = self.client.query(query).to_dataframe()

        if len(image) == 0:
            print("No features found in DB.")
            return


        print(image)
        # print(image)

        # image
        # if features_df.empty:
        #     print("No features found in DB.")
        #     return

        # image_df = pd.DataFrame(image)

        for row in image:
        # for i, row in features_df.iterrows():
            print(row)
            # fragment_id = int(row['id'])
            fragment_id = row[0]

            # Декодуємо PNG байти (row['image'] — це bytes)
            # image = cv2.imdecode(np.frombuffer(row['image'], dtype=np.uint8), cv2.IMREAD_COLOR)
            image = cv2.imdecode(np.frombuffer(row[1], dtype=np.uint8), cv2.IMREAD_COLOR)
            # Приводимо до RGB (якщо потрібно)
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            try:
                self.storage.add_fragment_with_id(fragment_id, image,
                                                  np.frombuffer(row[2], dtype=np.float32))
            except ValueError as e:
                print(e)

        self.build_tree()





    # ------------------
    # --- REFACTORED ---
    # ------------------
    def build_tree(self):
        features_dims = self.storage.get_random_fragment().features.shape[0]
        self.tree = AnnoyIndex(features_dims, 'euclidean')
        for i, fragment in self.storage.items():
            self.tree.add_item(i, fragment.features)

        n_trees = min(100, max(10, int(np.log2(len(self.storage))) * 5))
        self.tree.build(n_trees, n_jobs=-1)
        print("Tree was built")



    # ------------------
    # --- REFACTORED ---
    # ------------------
    def add_fragments(self, fragments: list[Fragment]) -> Optional[str]:
        if len(fragments) == 0:
            return "No fragments to add into base"
        rows = []

        print('__add_fragments')
        insert_query = '''
                       INSERT INTO image (id, image, features)
                       VALUES (%s, %s, %s) \
                       '''
        # Data to be inserted
        # user_data = [
        #     (1, 'John', 25),
        #     (2, 'Smith', 35),
        #     (3, 'Tom', 29)
        # ]

        # for user in user_data:
        #     cur.execute(insert_query, user)

        # conn.commit()

        for fragment in fragments:
            fragment_id = self.add_fragment(fragment)
            self.cur.execute(insert_query,(fragment_id,BigQueryDB.compress_nparr_to_bytes(fragment.image),fragment.features.tobytes()))
            # rows.append({
            #     "id": fragment_id,
            #     "image": BigQueryDB.compress_nparr_to_bytes(fragment.image),
            #     "features": fragment.features.tobytes()
            # })

        self.conn.commit()
        # fragments_df = pd.DataFrame(rows)
        # fragments_df['id'] = fragments_df['id'].astype(int)

        # try:
        #     job = self.client.load_table_from_dataframe(fragments_df, self.target_table,
        #                                                 job_config=BigQueryDB.TABLE_CONFIG)
        #     job.result()
        #     return "Successfully added fragments into base"
        #
        # except Exception as e:
        #     print(f"Error occurred while adding fragments to BigQuery: {e}")
        #     raise e



    # ------------------
    # --- REFACTORED ---
    # ------------------
    def add_fragment(self, fragment: Fragment, flag: bool = False):

        new_fragment_id = self.storage.add_fragment(fragment.image, fragment.features)
        if flag:
            self.buffer_fragments_ids.append(new_fragment_id)
        return new_fragment_id

    # ------------------
    # --- REFACTORED ---
    # ------------------
    @timing("Time updating fragments")
    def update_fragments(self):
        print('__update_fragments')

        insert_query = '''
                       UPDATE image SET image = %s, features = %s WHERE id = %s
                       '''

        if len(self.buffer_fragments_ids) == 0:
            return
        else:
            fragment_to_add = []
            for fragment_id in self.buffer_fragments_ids:
                fragment = self.storage.get_fragment(fragment_id)

                # fragment_to_add.append({
                #     "id": fragment_id,
                #     "image": BigQueryDB.compress_nparr_to_bytes(fragment.image),
                #     "features": fragment.features.tobytes()
                # })
                self.cur.execute(insert_query, (BigQueryDB.compress_nparr_to_bytes(fragment.image), fragment.features.tobytes(), fragment_id))

                # self.cur.execute(insert_query,
                #                  {
                #                      id: fragment_id,
                #                      image: BigQueryDB.compress_nparr_to_bytes(fragment.image),
                #                      features: fragment.features.tobytes()})
            # fragments_df = pd.DataFrame(fragment_to_add)
            # try:
            #     job = self.client.load_table_from_dataframe(fragments_df, self.target_table,
            #                                                 job_config=BigQueryDB.TABLE_CONFIG)
            #     job.result()
            #     print("Updated fragments loaded successfully")
            #     self.buffer_fragments_ids = []
            # except Exception as e:
            #     print(f"Error occurred while adding fragments to BigQuery: {e}")


    # ------------------
    # --- REFACTORED ---
    # ------------------
    def get_db_size(self) -> int:
        return len(self.storage)


    # ------------------
    # --- REFACTORED ---
    # ------------------
    def get_fragment_by_id(self, fragment_id: int) -> Fragment:
        print('__get_fragment_by_id')
        return self.storage.get_fragment(fragment_id)



    # ------------------
    # --- REFACTORED ---
    # ------------------
    def find_similar_fragment_id(self, fragment_feature) -> int:
        similar_fragment_id = self.tree.get_nns_by_vector(fragment_feature, 1)[0]
        return similar_fragment_id



    # ------------------
    # --- REFACTORED ---
    # ------------------
    def find_k_similar_fragments(self, fragment_feature, k) -> List[Fragment]:
        similar_fragment_ids = self.tree.get_nns_by_vector(fragment_feature, k)
        return [self.storage.get_fragment(fr_id) for fr_id in similar_fragment_ids]


    # ------------------
    # --- REFACTORED ---
    # ------------------
    @staticmethod
    def compress_nparr_to_bytes(nparr) -> bytes:
        # PNG-стиснення
        _, encoded_png = cv2.imencode('.jpg', cv2.cvtColor(nparr, cv2.COLOR_RGB2BGR))
        return encoded_png.tobytes()


    # ------------------
    # --- REFACTORED ---
    # ------------------
    def get_fragments_signed_url(self, fragments_base_name: str) -> str:
        return self.bucket_storage.get_signed_url(fragments_base_name)


    # ------------------
    # --- REFACTORED ---
    # ------------------
    def upload_new_fragments_base(self):
        return self.bucket_storage.add_fragments_to_minio(self.storage)
