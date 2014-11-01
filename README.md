Lucidworks YARN Integration Prototype
========

Solr on YARN - simple client / master needed to deploy SolrCloud into YARN.

Getting Started
========

1. Start ZooKeeper 3.4.6+ on your local workstation

2. Upload a Solr tgz bundle to HDFS

```
hdfs dfs -put solr-4.10.0.tgz /user/timpotter/
```

3. Upload the YARN Prototype JAR to HDFS

```
cd yarn-proto
mvn clean package
hdfs dfs -put target/yarn-proto-1.0-SNAPSHOT.jar /user/timpotter/
```

4. Create a directory in HDFS for Solr indexes:

```
hdfs dfs -mkdir solr_data
```

5. Deploy Solr on YARN

```
hadoop jar target/yarn-proto-1.0-SNAPSHOT.jar com.lucidworks.yarn.SolrClient \
  -nodes=2 \
  -zkHost=localhost:2181 \
  -solr=hdfs://localhost:9000/user/timpotter/solr-4.10.0.tgz \
  -jar=hdfs://localhost:9000/user/timpotter/yarn-proto-1.0-SNAPSHOT.jar \
  -memory 512 \
  -hdfs_home=hdfs://localhost:9000/user/timpotter/solr_data
```

This example will deploy a 2-node SolrCloud cluster using the ZooKeeper listening on localhost:2181, 
each node is allocated 512M max heap.

The client application will output log messages like:
```
14/11/01 08:52:25 INFO yarn.SolrClient: Submitting application application_1414852034829_0003
14/11/01 08:52:25 INFO impl.YarnClientImpl: Submitted application application_1414852034829_0003
14/11/01 08:52:35 INFO yarn.SolrClient: Solr (application_1414852034829_0003) is RUNNING
```

For details on how to use the SolrClient utility, do:

```
hadoop jar target/yarn-proto-1.0-SNAPSHOT.jar com.lucidworks.yarn.SolrClient --help
```

6. Navigate to the YARN ResourceManager Web UI @ http://localhost:8088/cluster

To stop the Solr application, use yarn application -list to find the APPID for the SolrCloud application:

```
yarn application -list
yarn application -kill <APPID>
```

Shutdown Handler
========

To enable graceful shutdown of Solr running in YARN, we need to deploy the com.lucidworks.yarn.ShutdownHandler
into the Jetty container:

A. Add the YarnShutdownHandler to the list of handlers in example/etc/jetty.xml:

```
<Item>
  <New id="YarnShutdownHandler" class="com.lucidworks.yarn.ShutdownHandler"/>
</Item>
```

NOTE: Order is important! The YarnShutdownHandler should be listed before the DefaultHandler.

B. Copy the yarn-proto-1.0-SNAPSHOT.jar to example/lib/ext/

Re-bundle into a tgz file (such as solr-4.10.0.tgz) and upload to HDFS
