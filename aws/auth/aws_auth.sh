#!/usr/bin/env bash
set -euo pipefail

# Goal:
# - One-time login per session duration using MFA to assume a role.
# - Cache the assumed-role credentials in the [default] profile in ~/.aws/credentials
#   so any terminal and any SDK (boto3, awscli) uses them automatically without
#   specifying a profile or keys.
# - Be smart: if cached creds are still valid, do NOT prompt for MFA again.
# - Avoid recursive role assumption caused by role_arn configured in [default].

# Expected AWS config/credentials (recommended):
# ~/.aws/credentials:
#   [base]                # Long-term IAM user keys (no session token)
#   aws_access_key_id=...
#   aws_secret_access_key=...
#
# ~/.aws/config:
#   [default]
#   region = us-east-1    # No role_arn here
#
#   [profile base]
#   mfa_serial = arn:aws:iam::<acct>:mfa/your.name
#
#   [profile role]        # Optional convenience profile to hold role_arn
#   role_arn = arn:aws:iam::<acct>:role/YourRole
#   source_profile = base

# You can customize these via env vars before calling the script.
BASE_PROFILE=${BASE_PROFILE:-base}
ROLE_PROFILE=${ROLE_PROFILE:-role}
TARGET_PROFILE=${TARGET_PROFILE:-default}
SESSION_NAME_PREFIX=${SESSION_NAME_PREFIX:-awslogin}
# Duration for the assumed role. Must be <= role MaxSessionDuration. Default 1h.
DURATION_SECONDS=${DURATION_SECONDS:-3600}

# Helpers
fail() { echo "❌ $*" >&2; exit 1; }
info() { echo "🔹 $*"; }
ok() { echo "✅ $*"; }
warn() { echo "⚠️  $*"; }

# Discover region; fall back to us-east-1
REGION=$(aws configure get region --profile "$TARGET_PROFILE" || true)
REGION=${REGION:-us-east-1}

# Helper to remove role_arn/source_profile from [default] (and [profile default]) in ~/.aws/config
cleanup_default_profile_config() {
  python3 - <<'PY'
import configparser, os
path = os.path.expanduser('~/.aws/config')
cp = configparser.RawConfigParser()
cp.read(path)
changed = False
for section in ('default', 'profile default'):
    if cp.has_section(section):
        for key in ('role_arn','source_profile'):
            if cp.has_option(section, key):
                cp.remove_option(section, key)
                changed = True
if changed:
    with open(path, 'w') as f:
        cp.write(f)
PY
}

# If role_arn is wrongly configured on [default], move it to ROLE_PROFILE to avoid recursive assume-role
DEFAULT_ROLE_ARN=$(aws configure get role_arn --profile "$TARGET_PROFILE" || true)
if [[ -n "${DEFAULT_ROLE_ARN}" ]]; then
  warn "role_arn is set on [$TARGET_PROFILE]; this causes the CLI/SDK to try assuming a role again using already-assumed creds (AccessDenied)."
  info "Moving role_arn and source_profile from [$TARGET_PROFILE] to [${ROLE_PROFILE}]..."
  aws configure set role_arn "$DEFAULT_ROLE_ARN" --profile "$ROLE_PROFILE"
  SRC_PROFILE=$(aws configure get source_profile --profile "$TARGET_PROFILE" || echo "$BASE_PROFILE")
  aws configure set source_profile "$SRC_PROFILE" --profile "$ROLE_PROFILE"
  # Fully remove keys from [default]
  cleanup_default_profile_config
  ok "role_arn moved to [${ROLE_PROFILE}]. [${TARGET_PROFILE}] now remains a plain profile."
fi

# Ensure no stray source_profile/role_arn remains on [default] even if role_arn wasn't present (e.g., empty value left behind)
cleanup_default_profile_config

# Determine role_arn to use: prefer ROLE_PROFILE, else empty (MFA session only)
ROLE_ARN=$(aws configure get role_arn --profile "$ROLE_PROFILE" || true)

# Read MFA serial from base profile
MFA_SERIAL=$(aws configure get mfa_serial --profile "$BASE_PROFILE" || true)
if [[ -z "$MFA_SERIAL" ]]; then
  fail "No mfa_serial found for base profile '$BASE_PROFILE' in ~/.aws/config (under [profile $BASE_PROFILE])."
fi

# 1) Fast path: if [default] already has working session credentials, reuse them
info "Checking for existing valid credentials in [$TARGET_PROFILE]..."
if AWS_PROFILE="$TARGET_PROFILE" aws sts get-caller-identity --output text >/dev/null 2>&1; then
  ok "Existing credentials for [$TARGET_PROFILE] are valid. Nothing to do."
  exit 0
fi

# 2) Need to authenticate: Ask for MFA and assume role (if configured)
echo "🔐 Using MFA device: $MFA_SERIAL"
read -r -p "Enter MFA code: " TOKEN_CODE

# Sanity check: base profile must be usable (long-term keys)
if ! AWS_PROFILE="$BASE_PROFILE" aws sts get-caller-identity --output text >/dev/null 2>&1; then
  fail "Base profile '$BASE_PROFILE' is not usable. Ensure ~/.aws/credentials has [${BASE_PROFILE}] with long-term keys."
fi

SESSION_NAME="${SESSION_NAME_PREFIX}-$(date +%s)"

if [[ -n "$ROLE_ARN" ]]; then
  info "Assuming role $ROLE_ARN with MFA (duration ${DURATION_SECONDS}s)..."
  ASSUME_OUTPUT=$(AWS_PROFILE="$BASE_PROFILE" aws sts assume-role \
    --role-arn "$ROLE_ARN" \
    --role-session-name "$SESSION_NAME" \
    --serial-number "$MFA_SERIAL" \
    --token-code "$TOKEN_CODE" \
    --duration-seconds "$DURATION_SECONDS" \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken,Expiration]' \
    --output text \
    --region "$REGION" || true)
else
  info "No role_arn configured on [${ROLE_PROFILE}]. Getting MFA session for base user (duration 12h)..."
  ASSUME_OUTPUT=$(AWS_PROFILE="$BASE_PROFILE" aws sts get-session-token \
    --serial-number "$MFA_SERIAL" \
    --token-code "$TOKEN_CODE" \
    --duration-seconds 43200 \
    --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken,Expiration]' \
    --output text \
    --region "$REGION" || true)
fi

if [[ -z "$ASSUME_OUTPUT" ]]; then
  fail "Failed to obtain temporary credentials (assume-role or get-session-token)."
fi

read -r ROLE_KEY ROLE_SECRET ROLE_TOKEN EXPIRATION <<<"$ASSUME_OUTPUT"

info "Writing session credentials to [$TARGET_PROFILE] (expires $EXPIRATION)..."
aws configure set aws_access_key_id "$ROLE_KEY" --profile "$TARGET_PROFILE"
aws configure set aws_secret_access_key "$ROLE_SECRET" --profile "$TARGET_PROFILE"
aws configure set aws_session_token "$ROLE_TOKEN" --profile "$TARGET_PROFILE"
aws configure set region "$REGION" --profile "$TARGET_PROFILE"

# Verify
if AWS_PROFILE="$TARGET_PROFILE" aws sts get-caller-identity --output text >/dev/null 2>&1; then
  ok "Logged in. Session active until $EXPIRATION for [$TARGET_PROFILE]."
else
  fail "Credentials were written but validation failed for [$TARGET_PROFILE]."
fi
