Wikimedia_Stream -> ELASTIC

This project focuses on pushing live data stream from Wikimeida Streams (which includes all the CRUD operations performed at Wikimedia and its child Domains).
Here I have used Apache Kafka (PUBSUB model) to gather these wikimedia infos in JSON format from Stream, using Event Source and Even Builder functionalities.

Later data from these Kafka topics are consumed using RestHighLevel Client and is pushed to Elastic Search indices (Bonsai hosted).
Most of Kafka optimisations are performed , from producer and consumer perspectives.
