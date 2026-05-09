AWS auth helper
================

What this does
--------------
- Performs MFA and assumes a selected role once, then caches the temporary credentials in your `~/.aws/credentials` under the `[default]` profile.
- All terminals and SDKs (AWS CLI, boto3) pick up the cached creds automatically without specifying a profile.
- If cached creds are still valid for the selected role, running `aws_auth.sh` again does not prompt for MFA.
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

	# Role used for Glue work
	[profile role-glue]
	role_arn = arn:aws:iam::<acct>:role/svc-glue-user-1
	source_profile = base

	# Role used for infrastructure provisioning work
	[profile role-infra]
	role_arn = arn:aws:iam::<acct>:role/svc-infra-deploy-role
	source_profile = base

Usage
-----
Run the script in any terminal:

	./aws_auth.sh

Behavior:
- Script prompts for role selection each run:
	- `1` (default): `glue` -> uses `[profile role-glue]`
	- `2`: `infra` -> uses `[profile role-infra]`
- If `[default]` already has valid credentials for the selected role, the script exits without prompting for MFA.
- If credentials are valid but for a different role, the script refreshes credentials for the role you selected.
- It writes the session keys to `[default]`, so subsequent shells and SDK calls work automatically, e.g.: `aws s3 ls` or any boto3 code with default config.

Notes
-----
- If `role_arn` is mistakenly set on `[default]`, the script relocates it to the selected role profile to avoid recursive role assumption.
- You can override variables via env vars:
	- `BASE_PROFILE`, `TARGET_PROFILE`, `DURATION_SECONDS`
	- `ROLE_PROFILE_GLUE`, `ROLE_PROFILE_INFRA`
	- `SELECTED_ROLE` (non-interactive override: `glue` or `infra`)
	- `DEFAULT_LOGICAL_ROLE` (`glue` or `infra`)

