# kafka-connect-timestamp-convertor

### This plugin convert type of field from string to timestamp

[Download Plugin](./plugin)


```
# For example: 

2025-06-04T12:01:48.156384Z' -> '2025-06-04 16:01:48.156 +0400

# Let's get start

git clone https://github.com/nicat-m/kafka-connect-timestamp-convertor.git

mvn clean package

# copy jar file under target folder to kafka connect plugin folder

cp -pr target/timestamp-convertor-1.0.jar /opt/kafka/plugins/

# edit kafka-connect properties

vim /opt/kafka/config/connect-distributed.properties

# set this path like this

plugin.path=/opt/kafka/plugins

# then restart your kafka connect

# sink connector config:

  "transforms": "convertTime"
  "transforms.convertTime.type": "az.devcloud.kafka.connect.convertor.ConvertToTimestamp",
  "transforms.convertTime.field.names": "transaction_date,insert_date",
  "transforms.convertTime.timezone": "Asia/Baku"
  "transforms.convertTime.input.format": "auto", 
  "transforms.convertTime.timezone": "Asia/Baku",
  
  # input format will be epoch_micro,auto,iso8601

  
```