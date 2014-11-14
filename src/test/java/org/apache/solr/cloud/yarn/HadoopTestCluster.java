package org.apache.solr.cloud.yarn;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

public class HadoopTestCluster {

  protected final static Log log = LogFactory.getLog(HadoopTestCluster.class);

  protected MiniYARNCluster yarnCluster = null;
  protected MiniDFSCluster dfsCluster = null;

  /** Unique cluster name */
  protected final String clusterName;

  /** Configuration build at runtime */
  protected Configuration configuration;

  /** Monitor sync for start and stop */
  protected final Object startupShutdownMonitor = new Object();

  /** Flag for cluster state */
  protected boolean started;

  /** Number of nodes for yarn and dfs */
  protected int nodes = 1;

  /**
   * Instantiates a mini cluster with default
   * cluster node count.
   *
   * @param clusterName the unique cluster name
   */
  public HadoopTestCluster(String clusterName) {
    this.clusterName = clusterName;
  }

  /**
   * Instantiates a mini cluster with given
   * cluster node count.
   *
   * @param clusterName the unique cluster name
   * @param nodes the node count
   */
  public HadoopTestCluster(String clusterName, int nodes) {
    this.clusterName = clusterName;
    this.nodes = nodes;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public MiniDFSCluster getDFSCluster() {
    return dfsCluster;
  }

  public void start() throws IOException {
    log.info("Checking if cluster=" + clusterName + " needs to be started");
    synchronized (this.startupShutdownMonitor) {
      if (started) {
        return;
      }
      log.info("Starting cluster=" + clusterName);
      configuration = new YarnConfiguration();

      //configuration.setBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, true);
      configuration.setBoolean("yarn.is.minicluster", true);
      configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "target/" + clusterName + "-dfs");

      dfsCluster = new MiniDFSCluster.Builder(configuration).
              numDataNodes(nodes).
              build();

      yarnCluster = new MiniYARNCluster(clusterName, nodes, 1, 1);
      yarnCluster.init(configuration);
      yarnCluster.start();

      log.info("Started cluster=" + clusterName);
      started = true;
    }
  }

  public void stop() {
    log.info("Checking if cluster=" + clusterName + " needs to be stopped");
    synchronized (this.startupShutdownMonitor) {
      if (!started) {
        return;
      }
      if (yarnCluster != null) {
        yarnCluster.stop();
        yarnCluster = null;
      }
      if (dfsCluster != null) {
        dfsCluster.shutdown();
        dfsCluster = null;
      }
      log.info("Stopped cluster=" + clusterName);
      started = false;
    }
  }

  public File getYarnWorkDir() {
    return yarnCluster != null ? yarnCluster.getTestWorkDir() : null;
  }

  /**
   * Sets a number of nodes for cluster. Every node
   * will act as yarn and dfs role. Default is one node.
   *
   * @param nodes the number of nodes
   */
  public void setNodes(int nodes) {
    this.nodes = nodes;
  }

}
