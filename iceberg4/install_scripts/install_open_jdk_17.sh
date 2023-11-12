#!/bin/bash

apt-get update --yes && \
apt-get install --yes --no-install-recommends \
"openjdk-17-jre-headless" \
ca-certificates-java && \
apt-get clean && rm -rf /var/lib/apt/lists/*