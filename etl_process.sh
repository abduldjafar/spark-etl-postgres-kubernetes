docker build -t kotekaman/etlpostgres -f etl.Dockerfile .
docker-compose run etl python main.py \
  --source /opt/data/transaction.csv \
  --database postgres \
  --table transactions