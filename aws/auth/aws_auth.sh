#!/usr/bin/env bash
set -euo pipefail

# Goal:
# - One-time login per session duration using MFA to assume a role.
# - Cache the assumed-role credentials in the [default] profile in ~/.aws/credentials
#   so any terminal and any SDK (boto3, awscli) uses them automatically without
#   specifying a profile or keys.
# - Be smart: if cached creds are still valid for the selected role, do NOT prompt
#   for MFA again.
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
#   [profile role-glue]   # Holds role_arn for svc-glue-user-1
#   role_arn = arn:aws:iam::<acct>:role/svc-glue-user-1
#   source_profile = base
#
#   [profile role-infra]  # Holds role_arn for svc-infra-deploy-role
#   role_arn = arn:aws:iam::<acct>:role/svc-infra-deploy-role
#   source_profile = base

# You can customize these via env vars before calling the script.
BASE_PROFILE=${BASE_PROFILE:-base}
TARGET_PROFILE=${TARGET_PROFILE:-default}
SESSION_NAME_PREFIX=${SESSION_NAME_PREFIX:-awslogin}
# Duration for the assumed role. Must be <= role MaxSessionDuration. Default 1h.
DURATION_SECONDS=${DURATION_SECONDS:-3600}
DEFAULT_LOGICAL_ROLE=${DEFAULT_LOGICAL_ROLE:-glue}
# Optional non-interactive override: glue|infra|svc-glue-user-1|svc-infra-deploy-role|1|2
SELECTED_ROLE=${SELECTED_ROLE:-}

# Role profile mapping. ROLE_PROFILE is kept for backward compatibility with older setups.
ROLE_PROFILE_GLUE=${ROLE_PROFILE_GLUE:-${ROLE_PROFILE:-role-glue}}
ROLE_PROFILE_INFRA=${ROLE_PROFILE_INFRA:-role-infra}

# Helpers
fail() { echo "❌ $*" >&2; exit 1; }
info() { echo "🔹 $*"; }
ok() { echo "✅ $*"; }
warn() { echo "⚠️  $*"; }

normalize_role_choice() {
  local raw
  raw=$(echo "$1" | tr '[:upper:]' '[:lower:]' | xargs)
  case "$raw" in
    ""|"1"|"glue"|"svc-glue-user-1") echo "glue" ;;
    "2"|"infra"|"svc-infra-deploy-role") echo "infra" ;;
    *) echo "" ;;
  esac
}

choose_logical_role() {
  local normalized
  if [[ -n "$SELECTED_ROLE" ]]; then
    normalized=$(normalize_role_choice "$SELECTED_ROLE")
    [[ -n "$normalized" ]] || fail "Invalid SELECTED_ROLE='$SELECTED_ROLE'. Use glue or infra."
    echo "$normalized"
    return 0
  fi

  echo "Choose target AWS role:" >&2
  if [[ "$DEFAULT_LOGICAL_ROLE" == "infra" ]]; then
    echo "  1) infra (svc-infra-deploy-role) [default]" >&2
    echo "  2) glue  (svc-glue-user-1)" >&2
    read -r -p "Selection [1]: " ROLE_CHOICE
    if [[ -z "$ROLE_CHOICE" ]]; then
      echo "infra"
      return 0
    fi
    normalized=$(normalize_role_choice "$ROLE_CHOICE")
    if [[ "$normalized" == "glue" ]]; then
      echo "glue"
    elif [[ "$normalized" == "infra" ]]; then
      echo "infra"
    else
      fail "Invalid selection '$ROLE_CHOICE'."
    fi
  else
    echo "  1) glue  (svc-glue-user-1) [default]" >&2
    echo "  2) infra (svc-infra-deploy-role)" >&2
    read -r -p "Selection [1]: " ROLE_CHOICE
    if [[ -z "$ROLE_CHOICE" ]]; then
      echo "glue"
      return 0
    fi
    normalized=$(normalize_role_choice "$ROLE_CHOICE")
    if [[ "$normalized" == "glue" ]]; then
      echo "glue"
    elif [[ "$normalized" == "infra" ]]; then
      echo "infra"
    else
      fail "Invalid selection '$ROLE_CHOICE'."
    fi
  fi
}

caller_matches_role() {
  local caller_arn="$1"
  local role_arn="$2"
  local expected_role_name
  expected_role_name="${role_arn##*/}"
  [[ "$caller_arn" == *":assumed-role/${expected_role_name}/"* ]]
}

resolve_selected_role_profile() {
  local logical_role="$1"
  case "$logical_role" in
    glue) echo "$ROLE_PROFILE_GLUE" ;;
    infra) echo "$ROLE_PROFILE_INFRA" ;;
    *) fail "Unknown logical role '$logical_role'." ;;
  esac
}

# Discover region; fall back to us-east-1
REGION=$(aws configure get region --profile "$TARGET_PROFILE" || true)
REGION=${REGION:-us-east-1}

SELECTED_LOGICAL_ROLE=$(choose_logical_role)
SELECTED_ROLE_PROFILE=$(resolve_selected_role_profile "$SELECTED_LOGICAL_ROLE")
info "Selected role: $SELECTED_LOGICAL_ROLE (profile [$SELECTED_ROLE_PROFILE])"

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

# If role_arn is wrongly configured on [default], move it to selected role profile to avoid recursive assume-role
DEFAULT_ROLE_ARN=$(aws configure get role_arn --profile "$TARGET_PROFILE" || true)
if [[ -n "${DEFAULT_ROLE_ARN}" ]]; then
  warn "role_arn is set on [$TARGET_PROFILE]; this causes the CLI/SDK to try assuming a role again using already-assumed creds (AccessDenied)."
  info "Moving role_arn and source_profile from [$TARGET_PROFILE] to [${SELECTED_ROLE_PROFILE}]..."
  aws configure set role_arn "$DEFAULT_ROLE_ARN" --profile "$SELECTED_ROLE_PROFILE"
  SRC_PROFILE=$(aws configure get source_profile --profile "$TARGET_PROFILE" || echo "$BASE_PROFILE")
  aws configure set source_profile "$SRC_PROFILE" --profile "$SELECTED_ROLE_PROFILE"
  # Fully remove keys from [default]
  cleanup_default_profile_config
  ok "role_arn moved to [${SELECTED_ROLE_PROFILE}]. [${TARGET_PROFILE}] now remains a plain profile."
fi

# Ensure no stray source_profile/role_arn remains on [default] even if role_arn wasn't present (e.g., empty value left behind)
cleanup_default_profile_config

# Determine role_arn to use for selected role profile
ROLE_ARN=$(aws configure get role_arn --profile "$SELECTED_ROLE_PROFILE" || true)
if [[ -z "$ROLE_ARN" ]]; then
  fail "No role_arn found in [${SELECTED_ROLE_PROFILE}] in ~/.aws/config. Configure the selected role profile first."
fi

# Read MFA serial from base profile
MFA_SERIAL=$(aws configure get mfa_serial --profile "$BASE_PROFILE" || true)
if [[ -z "$MFA_SERIAL" ]]; then
  fail "No mfa_serial found for base profile '$BASE_PROFILE' in ~/.aws/config (under [profile $BASE_PROFILE])."
fi

# 1) Fast path: if [default] already has working session credentials for selected role, reuse them
info "Checking for existing valid credentials in [$TARGET_PROFILE]..."
CALLER_ARN=$(AWS_PROFILE="$TARGET_PROFILE" aws sts get-caller-identity --query Arn --output text 2>/dev/null || true)
if [[ -n "$CALLER_ARN" ]]; then
  if caller_matches_role "$CALLER_ARN" "$ROLE_ARN"; then
    ok "Existing credentials for [$TARGET_PROFILE] are valid and already match selected role. Nothing to do."
    exit 0
  fi
  warn "Existing credentials are valid but for a different identity ($CALLER_ARN). Refreshing for selected role."
fi

# 2) Need to authenticate: Ask for MFA and assume role (if configured)
echo "🔐 Using MFA device: $MFA_SERIAL"
read -r -p "Enter MFA code: " TOKEN_CODE

# Sanity check: base profile must be usable (long-term keys)
if ! AWS_PROFILE="$BASE_PROFILE" aws sts get-caller-identity --output text >/dev/null 2>&1; then
  fail "Base profile '$BASE_PROFILE' is not usable. Ensure ~/.aws/credentials has [${BASE_PROFILE}] with long-term keys."
fi

SESSION_NAME="${SESSION_NAME_PREFIX}-$(date +%s)"

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

if [[ -z "$ASSUME_OUTPUT" ]]; then
  fail "Failed to obtain temporary credentials using assume-role for $ROLE_ARN."
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
