cd /home/changlu/pat/scala/ad
git pull
./sbt assembly
sudo mv target/scala-2.10/ad_2.10-1.0.0-SNAPSHOT.jar  /data/run/pat/test/
sudo su - hdfs
// hadoop fs -rmr /user/changlu/pixel_pat/
spark-submit --class com.everstring.ad.driver.AdScoringDriver --master yarn --deploy-mode client --queue etl --num-executors 7 --driver-memory 6g --executor-memory 6g --executor-cores 6 /data/run/pat/test/ad_2.10-1.0.0-SNAPSHOT.jar "analytics.pixel_trk_log_link_visit_action_pat" "2016-03-31 00:00:00" /user/changlu/pixel_pat/scores "845213" "5,6"

nohup spark-submit --class com.everstring.ad.driver.WebsiteVisitorAnalysisDriver --master yarn --deploy-mode client --queue ns_debug --num-executors 14 --driver-memory 8g --executor-memory 8g --executor-cores 6 /data/run/pat/test/ad_2.10-1.0.0-SNAPSHOT.jar "analytics.pixel_trk_log_link_visit_action_pat, analytics.pixel_trk_log_visit_pat, analytics.pixel_trk_log_action_pat" "2016-06-12 00:00:00" /user/changlu/pixel_pat/52f6393_1/ "845216,845213,1000069" "[[845216, 9], [845213, 10], [1000069, 00]]"  "/data/northstar/model_result" "staging" > /data/run/pat/orion.log 2>&1 &

