package org.apache.solr.cloud.yarn;

import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class BaseYarnTestCase {

  protected HadoopTestCluster testCluster;

  @Before
  public void setup() throws Exception {
    setupHadoopTestCluster();
  }

  @After
  public void tearDown() throws IOException {
    tearDownHadoopTestCluster();
  }

  protected FileSystem getFileSystem() throws IOException {
    return testCluster.getDFSCluster().getFileSystem();
  }

  protected void setupHadoopTestCluster() {
    int numNodes = getNumTestNodes();
    testCluster = new HadoopTestCluster(getClass().getSimpleName(), getNumTestNodes());
    try {
      testCluster.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to start Hadoop test cluster due to: "+e, e);
    }
  }

  protected int getNumTestNodes() {
    return 1;
  }

  protected void tearDownHadoopTestCluster() {
    testCluster.stop();
  }

}
