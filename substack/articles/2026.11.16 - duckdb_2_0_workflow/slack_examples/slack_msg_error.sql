INSTALL slack_msg from community; LOAD slack_msg;
create secret slack_creds (type slack, webhook_url getenv('SLACK_WEBHOOK'));

SET VARIABLE workflow_status = (
    SELECT map_from_entries(list((label, value) ORDER BY idx))
    FROM (VALUES
        (1, 'Workflow', 'DuckDB 2.0 ETL'),
        (2, 'Stage', 'Preparing source data'),
        (3, 'Status', 'Error'),
        (4, 'Error Details', 'Divide by zero encountered')
    ) AS data(idx, label, value)
);

CALL slack_post(
    secret_name => 'slack_creds',
    msg => 'The DuckDB ETL workflow encountered an error.',
    color => 'danger',
    title => 'ETL Workflow Update',
    fields => getvariable('workflow_status')
);