# Duck DB vs Polars comparison

Inspired by: https://grantmcdermott.com/duckdb-polars/

To download the NYC data you will need the
[aws cli tool](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
then you can run the following command:

```bash
mkdir -p nyc-taxi/year=2012
aws s3 cp s3://voltrondata-labs-datasets/nyc-taxi/year=2012 nyc-taxi/year=2012 --recursive --no-sign-request
```
