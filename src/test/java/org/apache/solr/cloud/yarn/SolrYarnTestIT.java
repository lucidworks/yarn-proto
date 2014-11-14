package org.apache.solr.cloud.yarn;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;

/**
 * Deploys a SolrCloud node on an embedded mini-Hadoop cluster. You need
 * to place a bundle (solr.tgz) of Solr into the project root directory before running this test.
 */
public class SolrYarnTestIT extends BaseYarnTestCase {

  protected String zkHost;
  protected TestingServer zk;

  @Before
  @Override
  public void setup() throws Exception {
    setupHadoopTestCluster();

    // Spin up a ZooKeeper
    zk = new TestingServer();
    zkHost = "localhost:"+zk.getPort();
  }

  @After
  @Override
  public void tearDown() throws IOException {
    zk.close();

    tearDownHadoopTestCluster();
  }

  @Test
  public void testClient() throws Exception {
    /*

     hadoop jar target/yarn-proto-1.0-SNAPSHOT.jar com.lucidworks.yarn.SolrClient \
      -nodes=2 \
      -zkHost=localhost:2181 \
      -solr=hdfs://localhost:9000/user/timpotter/solr-4.10.0.tgz \
      -jar=hdfs://localhost:9000/user/timpotter/yarn-proto-1.0-SNAPSHOT.jar \
      -memory 512 \
      -hdfs_home=hdfs://localhost:9000/user/timpotter/solr_data

     */
    File pwd = new File(".");

    FileSystem fs = getFileSystem();
    String uri = fs.getUri().toString();

    // Put Solr bundle in HDFS
    Path solrTgz = new Path("solr.tgz");
    fs.copyFromLocalFile(false, new Path("file://"+pwd.getAbsolutePath()+"/solr.tgz"), solrTgz);

    // create directory for HdfsDirectoryFactory
    Path indexDir = new Path("solr_data");
    fs.mkdirs(indexDir);

    // Put this project's JAR file into HDFS
    Path projectJar = new Path("yarn-proto-1.0-SNAPSHOT.jar");
    fs.copyFromLocalFile(false, new Path("file://"+pwd.getAbsolutePath()+"/target/yarn-proto-1.0-SNAPSHOT.jar"), projectJar);


    File extclasspath = new File("target/test-classes/mrapp-generated-classpath");
    String[] args = new String[] {
      "-nodes", "1",
      "-zkHost", zkHost,
      "-solr", solrTgz.makeQualified(fs).toString(),
      "-jar", projectJar.makeQualified(fs).toString(),
      "-memory", "512",
      "-hdfs_home", indexDir.makeQualified(fs).toString(),
      "-extclasspath", extclasspath.getAbsolutePath()
    };

    SolrClient.main(args, testCluster.getConfiguration());

    Thread.sleep(10000);

    // verify Solr is running
    CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
    cloudSolrServer.setDefaultCollection("collection1");
    cloudSolrServer.ping();

  }
}
