INSTALL slack_msg from community; LOAD slack_msg;
create secret slack_creds (type slack, webhook_url getenv('SLACK_WEBHOOK'));
CALL slack_post(secret_name => 'slack_creds', msg => 'DuckDB sent a msg to Slack!');