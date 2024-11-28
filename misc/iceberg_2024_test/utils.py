import os
import requests

def download_iceberg_jar(spark_version, scala_version, iceberg_version, target_dir):

    target_dir = os.path.expanduser(target_dir)

    # Construct the JAR URL
    base_url = "https://repo1.maven.org/maven2/org/apache/iceberg"
    jar_name = f"iceberg-spark-runtime-{spark_version}_{scala_version}-{iceberg_version}.jar"
    jar_url = f"{base_url}/iceberg-spark-runtime-{spark_version}_{scala_version}/{iceberg_version}/{jar_name}"
    
    jar_path = os.path.join(target_dir, jar_name)

    if os.path.exists(jar_path):
        return jar_path

    os.makedirs(target_dir, exist_ok=True)
    
    
    print(f"Downloading {jar_name} from {jar_url}...")
    response = requests.get(jar_url, stream=True)
    
    if response.status_code == 200:
        with open(jar_path, 'wb') as jar_file:
            for chunk in response.iter_content(chunk_size=8192):
                jar_file.write(chunk)
        print(f"Downloaded successfully: {jar_path}")
        return jar_path
    else:
        raise Exception(f"Failed to download {jar_name}. HTTP Status Code: {response.status_code}")