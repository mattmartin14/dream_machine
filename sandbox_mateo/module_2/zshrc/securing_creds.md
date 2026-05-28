To secure creds locally, I save them off in a non-git enabled local file as a json file. Below is an example JSON file that stores an aws user and private key

```json
{
    "aws_key":"abc12345",
    "private_key":"ccc124==g/gahflshfasdglhsglhign"
}
```

Then, in the .zshrc file, we can export environ vars that read this like so:

```bash
export AWS_KEY="$(jq -r '.aws_key' ~/misc/aws.json)"
export AWS_KEY="$(jq -r '.private_key' ~/misc/aws.json)"
```

Once this is done, any terminal or python code can read the environ var such as:

```python
import os
aws_key = os.getenv("AWS_KEY")
```

### Notes
If you do not have jq installed, use homebrew to install it via:

```bash
brew install jq
```

https://formulae.brew.sh/formula/jq

This package allows bash to parse json files