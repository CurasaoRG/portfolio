# заливаю файлы в Docker контейнер
CONTAINER_ID=14813da038fc
docker cp /home/yc-user/rg-folder/CA.pem $CONTAINER_ID:/data/
docker cp /home/yc-user/rg-folder/kafka.config $CONTAINER_ID:/data/
docker cp /home/yc-user/rg-folder/kafka_text.txt $CONTAINER_ID:/data/
docker cp /home/yc-user/rg-folder/project8.py $CONTAINER_ID:/data/

# Запускаю bash в контейнере
docker exec -ti $CONTAINER_ID bash 

# запускаю psql
docker exec -ti 353a4d5fd01a psql -h localhost -p 5432 -U jovyan

# запускаю kafka producer для отправки сообщений
TOPIC_IN=student.topic.cohort5.GRR.in
kafkacat -F /data/kafka.config -P -t $TOPIC_IN -K: 

# запускаю kafka consumer для контроля отправки сообщений
kafkacat -F /data/kafka.config -C -t $TOPIC_IN -f "key = %k \n payload = %s\n" 

# запускаю kafka consumer для контроля получения сообщений в выходном топике
TOPIC_OUT=student.topic.cohort5.GRR.out
kafkacat -F /data/kafka.config -C -t $TOPIC_OUT -f "key = %k \n payload = %s\n" 

# запускаю Python-скрипт
spark-submit --packages 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.1' /data/project8.py



docker cp /home/rg/Documents/Study/de-final-project/src 7ee014e4d7a5:/lessons/dags/