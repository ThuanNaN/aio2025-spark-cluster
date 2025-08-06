spark-submit \
  --master spark://192.168.1.101:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/compute_mean_salary.py