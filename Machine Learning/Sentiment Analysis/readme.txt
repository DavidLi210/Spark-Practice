This project performs SENTIMENT analysis
of particular hash tags in twitter data in real-time.

To use it:

Put Q1.py Q2.py Q3_xin.py Q4part1.py Q4part2.py into a new project
Q1:
Run the program directly and make sure there is no folder named as the output path in your project root directory
Q2:
Run the program directly
Q3:
Run the program directly
Q4:
1.# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

2.# Start Kafka Server
bin/kafka-server-start.sh config/server.properties

3.# Run built-in script to create new topic named "my-topic" with 1 partition on 1 node
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic my-topic

4.# Running Q4part1.py
python app.py

5.# To check if the data is written to kafka
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic my-topic --from-beginning

6.# Running streaming sentimental analysis,you can see the result as shown in the screenshot
python Q4part2.py
