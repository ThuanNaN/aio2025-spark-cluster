docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.authenticate=false \
  --conf spark.hadoop.security.authentication=simple \
  /opt/bitnami/spark/jobs/word_count.py