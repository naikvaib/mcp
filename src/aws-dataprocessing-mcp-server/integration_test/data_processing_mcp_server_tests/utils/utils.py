# utils/utils.py

# Mapping for converting snake_case parameters to camelCase
PARAMETER_MAPPING = {
    "job_name": "JobName",
    "database_name": "DatabaseName",
    "job_definition": "JobDefinition",
    "table_name": "Name",  # For get_table operation
    "cluster_id": "ClusterId",
    "step_id": "StepId",
    "policy_name": "PolicyName",
    "location_uri": "LocationUri",
    "role_name": "RoleName",
    "release_label": "ReleaseLabel",
    "step_concurrency_level": "StepConcurrencyLevel",
    "termination_protected": "TerminationProtected",
    "encryption_at_rest": "EncryptionAtRest",
    "code_content": "Body",
    "crawler_name": "Name",
    "trigger_name": "Name",
    "release_label": "ReleaseLabel",
    "step_concurrency_level": "StepConcurrencyLevel",
    "session_id": "Id",
    "description": "Description",
    "command": "Command",
    "classifier_name": "Name",
    "profile_name": "Name",
    "configuration": "Configuration",
}

# Map operation to resource type and param key
OPERATION_ARN_MAP = {
    "get_job": ("job", "job_name"),
    "get_database": ("database", "database_name"),
    "get_table": ("table", "database_name/table_name"),  # Concatenates database_name and table_name
    "get_connection": ("connection", "connection_name"),
    "get_work_group": ("workgroup", "name"),
    "get_data_catalog": ("datacatalog", "name"),
    "get_partition": ("partition", "database_name/table_name/partition_values"),
    "get_session": ("session", "session_id"),
    "get_crawler": ("crawler", "crawler_name"),
    "get_trigger": ("trigger", "trigger_name"),
    "get_workflow": ("workflow", "workflow_name"),
    "get_classifier": ("classifier", "classifier_name"),
    "get_usage_profile": ("usage-profile", "profile_name"),
}


# Mapping for operation prefixes to resource type and singular name
OPERATION_PREFIX_MAP = {
    "get_job": ("job_definition", "Job"),
    "get_database": ("", "Database"),
    "get_table": ("table_input", "Table"),
    "list_role_policies": ("", ""),
    "get_role": ("", "Role"),
    "describe_cluster": ("", "Cluster"),
    "get_data_catalog_encryption_settings": ("", "DataCatalogEncryptionSettings"),
    "get_object": ("", ""),  # S3 get_object operation
    "get_crawler": ("crawler_definition", "Crawler"),
    "get_data_catalog": ("", "DataCatalog"),
    "get_work_group": ("", "WorkGroup"),
    "get_session": ("", "Session"),
    "get_classifier": ("classifier_definition", "Classifier"),
    "get_usage_profile": ("", "UsageProfile"),
    # ... add more if needed
}

# Operations that should skip MCP tag validation
SKIP_TAG_CHECK_OPERATIONS = {
    "describe_step", "list_role_policies", "get_role_policy", "get_role", 
    "get_partition", "list_instance_groups", "list_instance_fleets",
    "get_data_catalog_encryption_settings", "get_object", "get_workflow_run", 
    "get_classifier", "get_usage_profile", "get_security_configuration"
}
