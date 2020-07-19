export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=1g"

./dev/make-distribution.sh --name spark-2.4.3 --tgz -Phadoop-2.6 -Phive -Pyarn
