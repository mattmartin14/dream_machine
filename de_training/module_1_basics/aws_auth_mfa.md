# AWS Authentication with MFA (Cross-Platform)

## Goal
Set up a reliable, secure AWS authentication flow with MFA that works on macOS, Linux, and Windows.

This guide keeps DuckDB lessons focused on SQL, while centralizing the one-time auth setup complexity.

## What This Module Uses
This repo includes a copied helper script at `scripts/aws_auth.py`.

The script obtains temporary credentials (with MFA) and refreshes them when needed.

## Why This Matters
- MFA improves account security
- Temporary credentials reduce risk compared with long-lived access
- A single auth command keeps S3 exercises smoother for beginners

## Step 1: Ensure Required Tooling
- Python managed through uv
- AWS CLI installed
- Valid AWS config/credentials baseline

## Step 2: Set MFA Device ARN
The helper expects an environment variable named `AWS_MFA_ARN`.

### macOS/Linux (bash or zsh)

```bash
export AWS_MFA_ARN='arn:aws:iam::123456789012:mfa/your.user'
```

### Windows PowerShell

```powershell
$env:AWS_MFA_ARN = 'arn:aws:iam::123456789012:mfa/your.user'
```

## Step 3: Run MFA Auth Helper
Use uv to run the script directly.

```bash
uv run python scripts/aws_auth.py
```

You will be prompted for an MFA code.

## Optional macOS Convenience Alias
If you use zsh and want a shortcut, you can define:

```bash
alias aws_auth='uv run python scripts/aws_auth.py'
```

This is optional. The uv command above is the cross-platform default.

## Step 4: Verify Credentials

```bash
aws sts get-caller-identity
```

If successful, continue with S3 examples in the DuckDB lesson.

## Bucket Name Environment Variable
For S3 examples in this module, set your bucket through env var `aws_bucket`.

### macOS/Linux

```bash
export aws_bucket='your-bucket-name'
```

### Windows PowerShell

```powershell
$env:aws_bucket = 'your-bucket-name'
```

## Troubleshooting
- If `AWS_MFA_ARN` is missing, set it and rerun.
- If credentials are expired, rerun the helper script.
- If AWS CLI cannot find credentials, run the helper again and then re-run `aws sts get-caller-identity`.
- If you switch shells or terminals, re-export env vars as needed.

## Mentor Note
Teach this as a reusable operational habit: authenticate securely first, validate identity second, then run data movement tasks.
