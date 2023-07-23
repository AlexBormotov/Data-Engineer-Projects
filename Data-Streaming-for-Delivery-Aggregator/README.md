# Data Streaming for Delivery Aggregator

### Discription

Delivery aggregator need to apllication which will deliver special offer with short time expiration to all nearby customers. 
We have data-streaming with nearest customers to the restaurant and data with the offers of all restaurants. 
We need to catch data-streming, compare with the offer dates and send a push-message with offer to customer if curent date and time is less then the offer validity period.

### Realization

1. Read data from Kafka using Spark Structured Streaming and Python in real time
2. Get a list of subscribers from the Postgres database.
3. Join data from Kafka with data from the database.
4. Store the received data in memory so as not to reassemble it after sending it to Postgres or Kafka.
5. Send an output message to Kafka with information about the promotion, the user with a list of favorites and the restaurant, and also insert records into Postgres to subsequently receive feedback from the user. The push notification service will read messages from Kafka and generate ready-made notifications.

### Repository structure

Directory `src` have folder `/scripts` - here is the main script of the project and 2 test scripts that were needed during the construction of the main script.

### Stek

`python`, `pyspark`, `spark sreaming`, `kafka`, `docker`, `postgresql`

### Project status: 

Done.
