package org.apache.solr.cloud.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.fail;

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

    Configuration hadoopConf = testCluster.getConfiguration();

    try {
      SolrClient.pingSolrCluster(zkHost, 1);
      fail("Ping SolrCloud "+zkHost+" before start-up should have failed!");
    } catch (Exception exc) {
      // this is expected as the cluster has not been started yet
    }

    SolrClient.main(args, hadoopConf);

    Thread.sleep(5000);

    // verify Solr is running
    SolrClient.pingSolrCluster(zkHost, 1);

    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(hadoopConf);
    yarnClient.start();

    List<ApplicationReport> apps = yarnClient.getApplications();
    for (ApplicationReport app : apps) {
      if ("SolrCloud".equals(app.getName())) {
        System.out.println("\n\n Killing SolrCloud application "+app.getApplicationId()+" \n\n");
        yarnClient.killApplication(app.getApplicationId());
        Thread.sleep(2000); // give a brief time for the stop to work
        break;
      }
    }

    yarnClient.stop();

    try {
      SolrClient.pingSolrCluster(zkHost, 1);
      fail("Ping SolrCloud "+zkHost+" after shutdown should have failed!");
    } catch (Exception exc) {
      // this is expected as the cluster is stopped
    }

    System.out.println("\n\n Solr on YARN Integration Test passed! Shutting down YARN cluster\n\n");
  }
}
