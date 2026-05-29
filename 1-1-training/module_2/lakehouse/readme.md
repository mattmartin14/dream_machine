creating local iceberg and delta deployments is pretty easy.

However, the minute you want to move to a cloud service, it becomes VERY involved. I wrote a post on this not too long ago [here](https://performancede.substack.com/p/spark-config-madness).

There is a ton of plumbing and nuances to handle running spark locally that is connected to a cloud object store. 