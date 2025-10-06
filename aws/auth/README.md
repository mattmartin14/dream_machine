AWS auth helper
================

What this does
--------------
- Performs MFA and assumes a role once, then caches the temporary credentials in your `~/.aws/credentials` under the `[default]` profile.
- All terminals and SDKs (AWS CLI, boto3) pick up the cached creds automatically without specifying a profile.
- If cached creds are still valid, running `aws_auth.sh` again does not prompt for MFA.
- Prevents the common "AccessDenied on assuming a role" loop caused by having `role_arn` configured on `[default]`.

Recommended AWS profiles
------------------------
`~/.aws/credentials`:

	[base]
	aws_access_key_id=AKIA...
	aws_secret_access_key=...

`~/.aws/config`:

	[default]
	region = us-east-1

	[profile base]
	mfa_serial = arn:aws:iam::<acct>:mfa/your.name

	# Optional convenience profile to store your target role
	[profile role]
	role_arn = arn:aws:iam::<acct>:role/YourRole
	source_profile = base

Usage
-----
Run the script in any terminal:

	./aws_auth.sh

Behavior:
- If `[default]` already has valid session credentials, the script exits without prompting.
- If not, it prompts for your MFA code and either:
  - assumes the role specified in `[profile role]`, or
  - falls back to a plain MFA session on your base user when no role_arn is configured.
- It writes the session keys to `[default]`, so subsequent shells and SDK calls work automatically, e.g.: `aws s3 ls` or any boto3 code with default config.

Notes
-----
- If `role_arn` is mistakenly set on `[default]`, the script relocates it to `[profile role]` to avoid recursive role assumption.
- You can override variables via env vars: `BASE_PROFILE`, `ROLE_PROFILE`, `TARGET_PROFILE`, `DURATION_SECONDS`.

