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
"""This module contains a Google Cloud Storage to BigQuery operator."""

import json
from typing import Optional, Sequence, Union
import warnings

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes
class GCSToBigQueryOperator(BaseOperator):
    """
    Loads files from Google Cloud Storage into BigQuery.

    The schema to be used for the BigQuery table may be specified in one of
    two ways. You may either directly pass the schema fields in, or you may
    point the operator to a Google Cloud Storage object name. The object in
    Google Cloud Storage must be a JSON file with the schema fields in it.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToBigQueryOperator`

    :param bucket: The bucket to load from. (templated)
    :type bucket: str
    :param source_objects: List of Google Cloud Storage URIs to load from. (templated)
        If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
    :type source_objects: list[str]
    :param destination_project_dataset_table: The dotted
        ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to load data into.
        If ``<project>`` is not included, project will be the project defined in
        the connection json. (templated)
    :type destination_project_dataset_table: str
    :param schema_fields: If set, the schema field list as defined here:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        Should not be set when source_format is 'DATASTORE_BACKUP'.
        Parameter must be defined if 'schema_object' is null and autodetect is False.
    :type schema_fields: list
    :param schema_object: If set, a GCS object path pointing to a .json file that
        contains the schema for the table. (templated)
        Parameter must be defined if 'schema_fields' is null and autodetect is False.
    :type schema_object: str
    :param source_format: File format to export.
    :type source_format: str
    :param compression: [Optional] The compression type of the data source.
        Possible values include GZIP and NONE.
        The default value is NONE.
        This setting is ignored for Google Cloud Bigtable,
        Google Cloud Datastore backups and Avro formats.
    :type compression: str
    :param create_disposition: The create disposition if the table doesn't exist.
    :type create_disposition: str
    :param skip_leading_rows: Number of rows to skip when loading from a CSV.
    :type skip_leading_rows: int
    :param write_disposition: The write disposition if the table already exists.
    :type write_disposition: str
    :param field_delimiter: The delimiter to use when loading from a CSV.
    :type field_delimiter: str
    :param max_bad_records: The maximum number of bad records that BigQuery can
        ignore when running the job.
    :type max_bad_records: int
    :param quote_character: The value that is used to quote data sections in a CSV file.
    :type quote_character: str
    :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
        extra values that are not represented in the table schema.
        If true, the extra values are ignored. If false, records with extra columns
        are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result.
    :type ignore_unknown_values: bool
    :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
    :type allow_quoted_newlines: bool
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
    :param encoding: The character encoding of the data. See:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).csvOptions.encoding
        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
    :param max_id_key: If set, the name of a column in the BigQuery table
        that's to be loaded. This will be used to select the MAX value from
        BigQuery after the load occurs. The results will be returned by the
        execute() command, which in turn gets stored in XCom for future
        operators to use. This can be helpful with incremental loads--during
        future executions, you can pick up from the max ID.
    :type max_id_key: str
    :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud and
        interact with the BigQuery service.
    :type bigquery_conn_id: str
    :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
        and interact with the Google Cloud Storage service.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: list
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. (Default: ``False``).
    :type external_table: bool
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
    :type time_partitioning: dict
    :param cluster_fields: Request that the result of this load be stored sorted
        by one or more columns. BigQuery supports clustering for both partitioned and
        non-partitioned tables. The order of columns given determines the sort order.
        Not applicable for external tables.
    :type cluster_fields: list[str]
    :param autodetect: [Optional] Indicates if we should automatically infer the
        options and schema for CSV and JSON sources. (Default: ``True``).
        Parameter must be setted to True if 'schema_fields' and 'schema_object' are undefined.
        It is suggested to set to True if table are create outside of Airflow.
    :type autodetect: bool
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    :param location: [Optional] The geographic location of the job. Required except for US and EU.
        See details at https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'bucket',
        'source_objects',
        'schema_object',
        'destination_project_dataset_table',
        'impersonation_chain',
    )
    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    # pylint: disable=too-many-locals,too-many-arguments
    @apply_defaults
    def __init__(
        self,
        *,
        bucket,
        source_objects,
        destination_project_dataset_table,
        schema_fields=None,
        schema_object=None,
        source_format='CSV',
        compression='NONE',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_EMPTY',
        field_delimiter=',',
        max_bad_records=0,
        quote_character=None,
        ignore_unknown_values=False,
        allow_quoted_newlines=False,
        allow_jagged_rows=False,
        encoding="UTF-8",
        max_id_key=None,
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default',
        delegate_to=None,
        schema_update_options=(),
        src_fmt_configs=None,
        external_table=False,
        time_partitioning=None,
        cluster_fields=None,
        autodetect=True,
        encryption_configuration=None,
        location=None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):

        super().__init__(**kwargs)

        if external_table:
            warnings.warn(
                "The external_table parameter has been deprecated. You should "
                "use the BigQueryCreateExternalTableOperator instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        # GCS config
        if src_fmt_configs is None:
            src_fmt_configs = {}
        if time_partitioning is None:
            time_partitioning = {}
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.ignore_unknown_values = ignore_unknown_values
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table
        self.encoding = encoding

        self.max_id_key = max_id_key
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning
        self.cluster_fields = cluster_fields
        self.autodetect = autodetect
        self.encryption_configuration = encryption_configuration
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        bq_hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        if not self.schema_fields:
            if self.schema_object and self.source_format != 'DATASTORE_BACKUP':
                gcs_hook = GCSHook(
                    gcp_conn_id=self.google_cloud_storage_conn_id,
                    delegate_to=self.delegate_to,
                    impersonation_chain=self.impersonation_chain,
                )
                blob = gcs_hook.download(
                    bucket_name=self.bucket,
                    object_name=self.schema_object,
                )
                schema_fields = json.loads(blob.decode("utf-8"))
            elif self.schema_object is None and self.autodetect is False:
                raise AirflowException(
                    'At least one of `schema_fields`, `schema_object`, or `autodetect` must be passed.'
                )
            else:
                schema_fields = None

        else:
            schema_fields = self.schema_fields

        source_uris = [f'gs://{self.bucket}/{source_object}' for source_object in self.source_objects]
        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        if self.external_table:
            cursor.create_external_table(
                external_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                compression=self.compression,
                skip_leading_rows=self.skip_leading_rows,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                encoding=self.encoding,
                src_fmt_configs=self.src_fmt_configs,
                encryption_configuration=self.encryption_configuration,
            )
        else:
            cursor.run_load(
                destination_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                autodetect=self.autodetect,
                create_disposition=self.create_disposition,
                skip_leading_rows=self.skip_leading_rows,
                write_disposition=self.write_disposition,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                ignore_unknown_values=self.ignore_unknown_values,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                encoding=self.encoding,
                schema_update_options=self.schema_update_options,
                src_fmt_configs=self.src_fmt_configs,
                time_partitioning=self.time_partitioning,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration,
            )

        if cursor.use_legacy_sql:
            escaped_table_name = f'[{self.destination_project_dataset_table}]'
        else:
            escaped_table_name = f'`{self.destination_project_dataset_table}`'

        if self.max_id_key:
            cursor.execute(f'SELECT MAX({self.max_id_key}) FROM {escaped_table_name}')
            row = cursor.fetchone()
            max_id = row[0] if row[0] else 0
            self.log.info(
                'Loaded BQ data with max %s.%s=%s',
                self.destination_project_dataset_table,
                self.max_id_key,
                max_id,
            )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_external_table(  # pylint: disable=too-many-locals,too-many-arguments
        self,
        external_project_dataset_table: str,
        schema_fields: List,
        source_uris: List,
        source_format: str = 'CSV',
        autodetect: bool = False,
        compression: str = 'NONE',
        ignore_unknown_values: bool = False,
        max_bad_records: int = 0,
        skip_leading_rows: int = 0,
        field_delimiter: str = ',',
        quote_character: Optional[str] = None,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        encoding: str = "UTF-8",
        src_fmt_configs: Optional[Dict] = None,
        labels: Optional[Dict] = None,
        encryption_configuration: Optional[Dict] = None,
        location: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """
        Creates a new external table in the dataset with the data from Google
        Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource

        for more details about these parameters.

        :param external_project_dataset_table:
            The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
            table name to create external table.
            If ``<project>`` is not included, project will be the
            project defined in the connection json.
        :type external_project_dataset_table: str
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
        :type schema_fields: list
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :type source_uris: list
        :param source_format: File format to export.
        :type source_format: str
        :param autodetect: Try to detect schema and format options automatically.
            Any option specified explicitly will be honored.
        :type autodetect: bool
        :param compression: [Optional] The compression type of the data source.
            Possible values include GZIP and NONE.
            The default value is NONE.
            This setting is ignored for Google Cloud Bigtable,
            Google Cloud Datastore backups and Avro formats.
        :type compression: str
        :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
            extra values that are not represented in the table schema.
            If true, the extra values are ignored. If false, records with extra columns
            are treated as bad records, and if there are too many bad records, an
            invalid error is returned in the job result.
        :type ignore_unknown_values: bool
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :type max_bad_records: int
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :type skip_leading_rows: int
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :type field_delimiter: str
        :param quote_character: The value that is used to quote data sections in a CSV
            file.
        :type quote_character: str
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
            (false).
        :type allow_quoted_newlines: bool
        :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
            The missing values are treated as nulls. If false, records with missing
            trailing columns are treated as bad records, and if there are too many bad
            records, an invalid error is returned in the job result. Only applicable when
            source_format is CSV.
        :type allow_jagged_rows: bool
        :param encoding: The character encoding of the data. See:

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
        :type encoding: str
        :param src_fmt_configs: configure optional fields specific to the source format
        :type src_fmt_configs: dict
        :param labels: a dictionary containing labels for the table, passed to BigQuery
        :type labels: dict
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
            **Example**: ::

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
                }
        :type encryption_configuration: dict
        """
        warnings.warn(
            "This method is deprecated. Please use `BigQueryHook.create_empty_table` method with"
            "pass passing the `table_resource` object. This gives more flexibility than this method.",
            DeprecationWarning,
        )
        location = location or self.location
        src_fmt_configs = src_fmt_configs or {}
        source_format = source_format.upper()
        compression = compression.upper()

        external_config_api_repr = {
            'autodetect': autodetect,
            'sourceFormat': source_format,
            'sourceUris': source_uris,
            'compression': compression,
            'ignoreUnknownValues': ignore_unknown_values,
        }

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            'skipLeadingRows': skip_leading_rows,
            'fieldDelimiter': field_delimiter,
            'quote': quote_character,
            'allowQuotedNewlines': allow_quoted_newlines,
            'allowJaggedRows': allow_jagged_rows,
            'encoding': encoding,
        }
        src_fmt_to_param_mapping = {'CSV': 'csvOptions', 'GOOGLE_SHEETS': 'googleSheetsOptions'}
        src_fmt_to_configs_mapping = {
            'csvOptions': [
                'allowJaggedRows',
                'allowQuotedNewlines',
                'fieldDelimiter',
                'skipLeadingRows',
                'quote',
                'encoding',
            ],
            'googleSheetsOptions': ['skipLeadingRows'],
        }
        if source_format in src_fmt_to_param_mapping.keys():
            valid_configs = src_fmt_to_configs_mapping[src_fmt_to_param_mapping[source_format]]
            src_fmt_configs = _validate_src_fmt_configs(
                source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
            )
            external_config_api_repr[src_fmt_to_param_mapping[source_format]] = src_fmt_configs

        # build external config
        external_config = ExternalConfig.from_api_repr(external_config_api_repr)
        if schema_fields:
            external_config.schema = [SchemaField.from_api_repr(f) for f in schema_fields]
        if max_bad_records:
            external_config.max_bad_records = max_bad_records

        # build table definition
        table = Table(table_ref=TableReference.from_string(external_project_dataset_table, project_id))
        table.external_data_configuration = external_config
        if labels:
            table.labels = labels

        if encryption_configuration:
            table.encryption_configuration = EncryptionConfiguration.from_api_repr(encryption_configuration)

        self.log.info('Creating external table: %s', external_project_dataset_table)
        self.create_empty_table(
            table_resource=table.to_api_repr(), project_id=project_id, location=location, exists_ok=True
        )
        self.log.info('External table created successfully: %s', external_project_dataset_table)

    def run_load(  # pylint: disable=too-many-locals,too-many-arguments,invalid-name
        self,
        destination_project_dataset_table: str,
        source_uris: List,
        schema_fields: Optional[List] = None,
        source_format: str = 'CSV',
        create_disposition: str = 'CREATE_IF_NEEDED',
        skip_leading_rows: int = 0,
        write_disposition: str = 'WRITE_EMPTY',
        field_delimiter: str = ',',
        max_bad_records: int = 0,
        quote_character: Optional[str] = None,
        ignore_unknown_values: bool = False,
        allow_quoted_newlines: bool = False,
        allow_jagged_rows: bool = False,
        encoding: str = "UTF-8",
        schema_update_options: Optional[Iterable] = None,
        src_fmt_configs: Optional[Dict] = None,
        time_partitioning: Optional[Dict] = None,
        cluster_fields: Optional[List] = None,
        autodetect: bool = False,
        encryption_configuration: Optional[Dict] = None,
    ) -> str:
        """
        Executes a BigQuery load command to load data from Google Cloud Storage
        to BigQuery. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param destination_project_dataset_table:
            The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
            table to load data into. If ``<project>`` is not included, project will be the
            project defined in the connection json. If a partition is specified the
            operator will automatically append the data, create a new partition or create
            a new DAY partitioned table.
        :type destination_project_dataset_table: str
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
            Required if autodetect=False; optional if autodetect=True.
        :type schema_fields: list
        :param autodetect: Attempt to autodetect the schema for CSV and JSON
            source files.
        :type autodetect: bool
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :type source_uris: list
        :param source_format: File format to export.
        :type source_format: str
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: str
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :type skip_leading_rows: int
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: str
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :type field_delimiter: str
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :type max_bad_records: int
        :param quote_character: The value that is used to quote data sections in a CSV
            file.
        :type quote_character: str
        :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
            extra values that are not represented in the table schema.
            If true, the extra values are ignored. If false, records with extra columns
            are treated as bad records, and if there are too many bad records, an
            invalid error is returned in the job result.
        :type ignore_unknown_values: bool
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
            (false).
        :type allow_quoted_newlines: bool
        :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
            The missing values are treated as nulls. If false, records with missing
            trailing columns are treated as bad records, and if there are too many bad
            records, an invalid error is returned in the job result. Only applicable when
            source_format is CSV.
        :type allow_jagged_rows: bool
        :param encoding: The character encoding of the data.

            .. seealso::
                https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
        :type encoding: str
        :param schema_update_options: Allows the schema of the destination
            table to be updated as a side effect of the load job.
        :type schema_update_options: Union[list, tuple, set]
        :param src_fmt_configs: configure optional fields specific to the source format
        :type src_fmt_configs: dict
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and  expiration as per API specifications.
        :type time_partitioning: dict
        :param cluster_fields: Request that the result of this load be stored sorted
            by one or more columns. BigQuery supports clustering for both partitioned and
            non-partitioned tables. The order of columns given determines the sort order.
        :type cluster_fields: list[str]
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
            **Example**: ::

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
                }
        :type encryption_configuration: dict
        """
        warnings.warn(
            "This method is deprecated. Please use `BigQueryHook.insert_job` method.", DeprecationWarning
        )

        if not self.project_id:
            raise ValueError("The project_id should be set")

        # To provide backward compatibility
        schema_update_options = list(schema_update_options or [])

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat # noqa # pylint: disable=line-too-long

        if schema_fields is None and not autodetect:
            raise ValueError('You must either pass a schema or autodetect=True.')

        if src_fmt_configs is None:
            src_fmt_configs = {}

        source_format = source_format.upper()
        allowed_formats = [
            "CSV",
            "NEWLINE_DELIMITED_JSON",
            "AVRO",
            "GOOGLE_SHEETS",
            "DATASTORE_BACKUP",
            "PARQUET",
        ]
        if source_format not in allowed_formats:
            raise ValueError(
                "{} is not a valid source format. "
                "Please use one of the following types: {}".format(source_format, allowed_formats)
            )

        # bigquery also allows you to define how you want a table's schema to change
        # as a side effect of a load
        # for more details:
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schemaUpdateOptions
        allowed_schema_update_options = ['ALLOW_FIELD_ADDITION', "ALLOW_FIELD_RELAXATION"]
        if not set(allowed_schema_update_options).issuperset(set(schema_update_options)):
            raise ValueError(
                "{} contains invalid schema update options."
                "Please only use one or more of the following options: {}".format(
                    schema_update_options, allowed_schema_update_options
                )
            )

        destination_project, destination_dataset, destination_table = _split_tablename(
            table_input=destination_project_dataset_table,
            default_project_id=self.project_id,
            var_name='destination_project_dataset_table',
        )

        configuration = {
            'load': {
                'autodetect': autodetect,
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                },
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'writeDisposition': write_disposition,
                'ignoreUnknownValues': ignore_unknown_values,
            }
        }

        time_partitioning = _cleanse_time_partitioning(destination_project_dataset_table, time_partitioning)
        if time_partitioning:
            configuration['load'].update({'timePartitioning': time_partitioning})

        if cluster_fields:
            configuration['load'].update({'clustering': {'fields': cluster_fields}})

        if schema_fields:
            configuration['load']['schema'] = {'fields': schema_fields}

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError(
                    "schema_update_options is only "
                    "allowed if write_disposition is "
                    "'WRITE_APPEND' or 'WRITE_TRUNCATE'."
                )
            else:
                self.log.info("Adding experimental 'schemaUpdateOptions': %s", schema_update_options)
                configuration['load']['schemaUpdateOptions'] = schema_update_options

        if max_bad_records:
            configuration['load']['maxBadRecords'] = max_bad_records

        if encryption_configuration:
            configuration["load"]["destinationEncryptionConfiguration"] = encryption_configuration

        src_fmt_to_configs_mapping = {
            'CSV': [
                'allowJaggedRows',
                'allowQuotedNewlines',
                'autodetect',
                'fieldDelimiter',
                'skipLeadingRows',
                'ignoreUnknownValues',
                'nullMarker',
                'quote',
                'encoding',
            ],
            'DATASTORE_BACKUP': ['projectionFields'],
            'NEWLINE_DELIMITED_JSON': ['autodetect', 'ignoreUnknownValues'],
            'PARQUET': ['autodetect', 'ignoreUnknownValues'],
            'AVRO': ['useAvroLogicalTypes'],
        }

        valid_configs = src_fmt_to_configs_mapping[source_format]

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {
            'skipLeadingRows': skip_leading_rows,
            'fieldDelimiter': field_delimiter,
            'ignoreUnknownValues': ignore_unknown_values,
            'quote': quote_character,
            'allowQuotedNewlines': allow_quoted_newlines,
            'encoding': encoding,
        }

        src_fmt_configs = _validate_src_fmt_configs(
            source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
        )

        configuration['load'].update(src_fmt_configs)

        if allow_jagged_rows:
            configuration['load']['allowJaggedRows'] = allow_jagged_rows

        job = self.insert_job(configuration=configuration, project_id=self.project_id)
        self.running_job_id = job.job_id
        return job.job_id

    def _cleanse_time_partitioning(
        destination_dataset_table: Optional[str], time_partitioning_in: Optional[Dict]
    ) -> Dict:  # if it is a partitioned table ($ is in the table name) add partition load option

        if time_partitioning_in is None:
            time_partitioning_in = {}

        time_partitioning_out = {}
        if destination_dataset_table and '$' in destination_dataset_table:
            time_partitioning_out['type'] = 'DAY'
        time_partitioning_out.update(time_partitioning_in)
        return time_partitioning_out

    def _split_tablename(
        table_input: str, default_project_id: str, var_name: Optional[str] = None
    ) -> Tuple[str, str, str]:

        if '.' not in table_input:
            raise ValueError(f'Expected table name in the format of <dataset>.<table>. Got: {table_input}')

        if not default_project_id:
            raise ValueError("INTERNAL: No default project is specified")

        def var_print(var_name):
            if var_name is None:
                return ""
            else:
                return f"Format exception for {var_name}: "

        if table_input.count('.') + table_input.count(':') > 3:
            raise Exception(
                '{var}Use either : or . to specify project '
                'got {input}'.format(var=var_print(var_name), input=table_input)
            )
        cmpt = table_input.rsplit(':', 1)
        project_id = None
        rest = table_input
        if len(cmpt) == 1:
            project_id = None
            rest = cmpt[0]
        elif len(cmpt) == 2 and cmpt[0].count(':') <= 1:
            if cmpt[-1].count('.') != 2:
                project_id = cmpt[0]
                rest = cmpt[1]
        else:
            raise Exception(
                '{var}Expect format of (<project:)<dataset>.<table>, '
                'got {input}'.format(var=var_print(var_name), input=table_input)
            )

        cmpt = rest.split('.')
        if len(cmpt) == 3:
            if project_id:
                raise ValueError(f"{var_print(var_name)}Use either : or . to specify project")
            project_id = cmpt[0]
            dataset_id = cmpt[1]
            table_id = cmpt[2]

        elif len(cmpt) == 2:
            dataset_id = cmpt[0]
            table_id = cmpt[1]
        else:
            raise Exception(
                '{var}Expect format of (<project.|<project:)<dataset>.<table>, '
                'got {input}'.format(var=var_print(var_name), input=table_input)
            )

        if project_id is None:
            if var_name is not None:
                log.info(
                    'Project not included in %s: %s; using project "%s"',
                    var_name,
                    table_input,
                    default_project_id,
                )
            project_id = default_project_id

        return project_id, dataset_id, table_id

    def _validate_src_fmt_configs(
        source_format: str,
        src_fmt_configs: dict,
        valid_configs: List[str],
        backward_compatibility_configs: Optional[Dict] = None,
    ) -> Dict:
        """
        Validates the given src_fmt_configs against a valid configuration for the source format.
        Adds the backward compatibility config to the src_fmt_configs.

        :param source_format: File format to export.
        :type source_format: str
        :param src_fmt_configs: Configure optional fields specific to the source format.
        :type src_fmt_configs: dict
        :param valid_configs: Valid configuration specific to the source format
        :type valid_configs: List[str]
        :param backward_compatibility_configs: The top-level params for backward-compatibility
        :type backward_compatibility_configs: dict
        """
        if backward_compatibility_configs is None:
            backward_compatibility_configs = {}

        for k, v in backward_compatibility_configs.items():
            if k not in src_fmt_configs and k in valid_configs:
                src_fmt_configs[k] = v

        for k, v in src_fmt_configs.items():
            if k not in valid_configs:
                raise ValueError(f"{k} is not a valid src_fmt_configs for type {source_format}.")

        return src_fmt_configs
