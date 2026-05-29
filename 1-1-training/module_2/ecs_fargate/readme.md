### Overview
ECS Fargate is an AWS serverless container platform. It makes running containers on AWS very easy, and it's way less involved than Kubernetes.

You can use ECS fargate as an alternative to AWS glue to deploy ETL scripts that do not require spark. This significantly reduces the overall costs.

I had actaully recently written a couple extensive blog posts detailing how to thoughtfully deploy AWS ECS Fargate ETL tasks below:

- [AWS ETL End To End](https://performancede.substack.com/p/complete-end-to-end-build-of-etl)
- [AWS ETL End To End Part 2](https://performancede.substack.com/p/aws-duck-db-full-etl-walk-through)


### What Really Matters?
AWS ECS removes a major barrier that teams find on AWS Glue. Glue offers several versions that are tightly coupled to Spark, but given a lot of workloads these days do not need distributed compute, you are left in Glue with 1 option - a python 3.9 runtime
- that is signficantly limiting
- Python 3.9 went out of support last year


With AWS ECS Fargate, we regain control of what python runtime we want to deploy. And we can make it reusable! Which is a huge value-add


