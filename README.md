# Recommandation Movie 
This project consists of developing of a Movie Recommendation System for Big Data. 
Scalable development using Spark ML (Machine Learning)/scala, and Cassandra technologies.


In this project I used machine learning, applying the Spark ML collaborative filtering model. The system consists of an API Rest, with two endpoints. The first point trains the model, the second point returns a list of movie recommendations,

## Cassandra
### Docker and configuring Cassandra:

The Cassandra version used was 3.11.6

```$ docker pull cassandra```

We will execute the docker run command to create new containers from pulled images:

```docker run --name cassandra-movie -p 127.0.0.1:9042:9042 -p 127.0.0.1:9160:9160 -v /target/database/data:/var/lib/cassandra -d cassandra```

Remark: We can mount Cassandra data directory /var/lib/cassandra on a persistent volume. This way the installation remains intact across container restarts

To get a bash shell in the container (cassandra-movie), use this command:

```docker exec -it <container Id> bash```
