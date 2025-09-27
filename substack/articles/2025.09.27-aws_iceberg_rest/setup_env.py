import boto3
import configparser
import os
from datetime import datetime, timezone

CREDENTIALS_FILE = os.path.expanduser("~/.aws/credentials")
CONFIG_FILE = os.path.expanduser("~/.aws/config")
DEFAULT_PROFILE = "default"      
PROFILE_NAME = "mfa-session"
ROLE_PROFILE_NAME = "assumed-role-session"  # Profile for assumed role credentials
# Use a different profile for permanent credentials
PERMANENT_PROFILE = "permanent"  # Profile with permanent IAM user credentials      
MFA_SERIAL = os.getenv("AWS_MFA_ARN")


def creds_valid(creds_dict):
    """Check if cached credentials are still valid."""
    expiration_str = creds_dict.get("expiration")
    if not expiration_str:
        return False
    try:
        expiration = datetime.fromisoformat(expiration_str)
    except ValueError:
        return False
    return expiration > datetime.now(timezone.utc)


def get_role_arn_from_config():
    """Get the role ARN from AWS config file."""
    if not os.path.exists(CONFIG_FILE):
        return None
    
    config = configparser.RawConfigParser()
    config.read(CONFIG_FILE)
    
    # Check both [default] and [profile default] sections
    for section_name in ['default', 'profile default']:
        if section_name in config:
            section = config[section_name]
            if 'role_arn' in section:
                return section['role_arn']
    
    return None


def load_profile():
    """Load cached MFA credentials if they exist and are valid."""
    config = configparser.RawConfigParser()
    if not os.path.exists(CREDENTIALS_FILE):
        return None

    config.read(CREDENTIALS_FILE)
    if PROFILE_NAME not in config:
        return None

    creds = dict(config[PROFILE_NAME])
    if creds_valid(creds):
        return creds
    return None


def load_role_profile():
    """Load cached assumed role credentials if they exist and are valid."""
    config = configparser.RawConfigParser()
    if not os.path.exists(CREDENTIALS_FILE):
        return None

    config.read(CREDENTIALS_FILE)
    if ROLE_PROFILE_NAME not in config:
        return None

    creds = dict(config[ROLE_PROFILE_NAME])
    if creds_valid(creds):
        return creds
    return None


def save_profile(creds, profile_name=PROFILE_NAME, credential_type="MFA session"):
    """Save temporary credentials to ~/.aws/credentials under specified profile."""
    
    config = configparser.RawConfigParser()
    if os.path.exists(CREDENTIALS_FILE):
        config.read(CREDENTIALS_FILE)

    # Convert datetime to ISO string for storage
    expiration_str = creds["Expiration"].isoformat()
    
    config[profile_name] = {
        "aws_access_key_id": creds["AccessKeyId"],
        "aws_secret_access_key": creds["SecretAccessKey"],
        "aws_session_token": creds["SessionToken"],
        "expiration": expiration_str,
    }

    with open(CREDENTIALS_FILE, "w") as f:
        config.write(f)
    
    print(f"Saved {credential_type} credentials to {profile_name} profile (expires: {expiration_str})")


def assume_role_with_mfa(mfa_session, role_arn):
    """Assume a role using MFA session credentials."""
    sts = mfa_session.client("sts")
    
    # Generate a session name
    import time
    session_name = f"mfa-assumed-role-{int(time.time())}"
    
    print(f"Assuming role: {role_arn}")
    
    response = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        DurationSeconds=3600,  # 1 hour for assumed role (max for chained assumption)
    )
    
    return response["Credentials"]


def set_aws_env_vars(creds):
    """Set AWS environment variables so all AWS SDKs/tools can use the credentials."""
    os.environ['AWS_ACCESS_KEY_ID'] = creds['aws_access_key_id']
    os.environ['AWS_SECRET_ACCESS_KEY'] = creds['aws_secret_access_key']
    
    # Handle session token if present (for temporary credentials)
    if 'aws_session_token' in creds and creds['aws_session_token']:
        os.environ['AWS_SESSION_TOKEN'] = creds['aws_session_token']
    else:
        # Remove session token if it exists (for permanent credentials)
        os.environ.pop('AWS_SESSION_TOKEN', None)
    
    print(f"Set AWS environment variables for: {creds['aws_access_key_id'][:8]}...")


def get_session():
    """Return a boto3 session using cached role credentials, or create new ones with MFA.
    Also sets AWS environment variables so all AWS operations use the same credentials."""
    
    # Check if we should assume a role
    role_arn = get_role_arn_from_config()
    
    if role_arn:
        # Check for cached valid role credentials first
        role_creds = load_role_profile()
        if role_creds:
            print(f"Using cached assumed role credentials (expires: {role_creds['expiration']})")
            set_aws_env_vars(role_creds)
            return boto3.Session(profile_name=ROLE_PROFILE_NAME)
    
    # Check for cached MFA credentials (needed for role assumption or direct use)
    mfa_creds = load_profile()
    mfa_session = None
    
    if mfa_creds:
        print(f"Using cached MFA credentials (expires: {mfa_creds['expiration']})")
        mfa_session = boto3.Session(profile_name=PROFILE_NAME)
    else:
        # Need to get new MFA session
        if not MFA_SERIAL:
            raise ValueError("AWS_MFA_ARN environment variable must be set")
        
        print("No valid cached MFA session found. Requesting new MFA token...")
        
        # Read credentials directly from file to bypass role assumption
        config = configparser.RawConfigParser()
        config.read(CREDENTIALS_FILE)
        
        if DEFAULT_PROFILE not in config:
            raise ValueError(f"Profile {DEFAULT_PROFILE} not found in {CREDENTIALS_FILE}")
        
        default_creds = dict(config[DEFAULT_PROFILE])
        
        # Create session using raw IAM user credentials (bypassing any role assumption)
        base_session = boto3.Session(
            aws_access_key_id=default_creds['aws_access_key_id'],
            aws_secret_access_key=default_creds['aws_secret_access_key']
            # Explicitly not including aws_session_token to ensure we use permanent creds
        )
        
        sts = base_session.client("sts")
        
        # Verify these are permanent credentials
        try:
            identity = sts.get_caller_identity()
            if ':assumed-role/' in identity['Arn']:
                raise ValueError("Still using temporary credentials somehow")
            print(f"Using IAM user: {identity['Arn']}")
        except Exception as e:
            raise ValueError(f"Failed to verify permanent credentials: {e}")

        token_code = input("Enter MFA code: ")
        response = sts.get_session_token(
            DurationSeconds=43200,  # 12 hours
            SerialNumber=MFA_SERIAL,
            TokenCode=token_code,
        )
        
        creds = response["Credentials"]
        save_profile(creds)
        
        # Create MFA session for role assumption
        mfa_session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"]
        )
    
    # If we have a role to assume, assume it
    if role_arn and mfa_session:
        try:
            role_creds = assume_role_with_mfa(mfa_session, role_arn)
            save_profile(role_creds, ROLE_PROFILE_NAME, "assumed role")
            
            # Convert to the format expected by set_aws_env_vars
            env_creds = {
                'aws_access_key_id': role_creds["AccessKeyId"],
                'aws_secret_access_key': role_creds["SecretAccessKey"],
                'aws_session_token': role_creds["SessionToken"]
            }
            
            # Set environment variables so Spark and other tools can use these credentials
            set_aws_env_vars(env_creds)

            # Create session using the assumed role credentials
            return boto3.Session(
                aws_access_key_id=role_creds["AccessKeyId"],
                aws_secret_access_key=role_creds["SecretAccessKey"],
                aws_session_token=role_creds["SessionToken"]
            )
        except Exception as e:
            print(f"Failed to assume role {role_arn}: {e}")
            print("Falling back to MFA session without role assumption")
    
    # Fallback: use MFA session directly (no role assumption)
    if mfa_session:
        mfa_creds = load_profile()
        env_creds = {
            'aws_access_key_id': mfa_creds['aws_access_key_id'],
            'aws_secret_access_key': mfa_creds['aws_secret_access_key'],
            'aws_session_token': mfa_creds['aws_session_token']
        }
        set_aws_env_vars(env_creds)
        return mfa_session
    
    raise ValueError("Failed to create any valid session")


def setup_aws_environment():
    """Convenience function to set up AWS environment for use in other scripts.
    
    Call this at the start of your scripts to ensure AWS credentials are available
    for all AWS operations (boto3, Spark, etc.)
    
    Returns:
        boto3.Session: Configured session with MFA credentials
    """
    return get_session()


if __name__ == "__main__":
    try:
        session = get_session()
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print("Successfully authenticated!")
        print("Caller identity:", identity)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()