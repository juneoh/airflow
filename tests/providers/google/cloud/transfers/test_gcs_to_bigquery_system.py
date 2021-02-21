#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest

from airflow.providers.google.cloud.example_dags.example_gcs_to_bigquery import DATASET_NAME
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_BIGQUERY_KEY)
class TestGoogleCloudStorageToBigQueryExample(GoogleSystemTest):
    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def test_run_example_dag_gcs_to_bigquery_operator(self):
        self.run_dag('example_gcs_to_bigquery_operator', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def setUp(self):
        super().setUp()
        cmd = [
            'bq',
            'mk',
            '--dataset',
            DATASET_NAME,
        ]
        self.execute_with_ctx(cmd, key=GCP_BIGQUERY_KEY)

    @provide_gcp_context(GCP_BIGQUERY_KEY)
    def tearDown(self):
        cmd = [
            'bq',
            'rm',
            '-r',
            '-f',
            '--dataset',
            DATASET_NAME,
        ]
        self.execute_with_ctx(cmd, key=GCP_BIGQUERY_KEY)
        super().tearDown()
