To secure creds locally, I save them off in a non-git enabled local file as a json file. Below is an example JSON file that stores an aws user and private key

```json
{
    "aws_key":"abc12345",
    "private_key":"ccc124==g/gahflshfasdglhsglhign"
}
```

Next, open the zshrc file as follows in terminal:

```bash
open ~/.zshrc
```

Then, in the .zshrc file, we can export environ vars that read this like so:

```bash
export AWS_KEY="$(jq -r '.aws_key' ~/misc/aws.json)"
export AWS_KEY="$(jq -r '.private_key' ~/misc/aws.json)"
```

After making the adjustment, save the zshrc file and then reload it so the updated environ vars are available to the terminal window you are working in. To reload the zshrc file, run this command:

```bash
source ~/.zshrc
```

Once this is done, any reloaded or new terminal or python code can read the environ var such as:

```python
import os
aws_key = os.getenv("AWS_KEY")
```

### Production Deployment
For production, use the cloud's native secrets manager for storing creds
However, most AWS services can auth via IAM, and most of AWS's managed services already have an auto discovery feature in them to pick up temorary aws credentials; thus, a secrets manager is only needed if you need to secure programmtic credentials to a database such as RDS Postgres

### Notes
If you do not have jq installed, use homebrew to install it via:

```bash
brew install jq
```

https://formulae.brew.sh/formula/jq

This package allows bash to parse json files