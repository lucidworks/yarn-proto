package org.apache.solr.cloud.yarn;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.apache.log4j.Logger;

/**
 * Launches Solr nodes in SolrCloud mode in YARN containers and then waits for shutdown.
 */
public class SolrMaster implements AMRMClientAsync.CallbackHandler {

  public static Logger log = Logger.getLogger(SolrMaster.class);

  public static void main(String[] args) throws Exception {
    (new SolrMaster(SolrClient.processCommandLineArgs(SolrMaster.class.getName(), getOptions(), args))).run();
  }

  CommandLine cli;
  Configuration conf;
  NMClient nmClient;
  int numContainersToWaitFor;
  int memory;
  int port;
  int nextPort;
  boolean isShutdown = false;
  String randomStopKey;
  Map<String, Set<Integer>> solrHosts = new HashMap<String, Set<Integer>>();
  String inetAddresses;

  public SolrMaster(CommandLine cli) throws Exception {
    this.cli = cli;
    Configuration hadoopConf = new Configuration();
    if (cli.hasOption("conf")) {
      hadoopConf.addResource(new Path(cli.getOptionValue("conf")));
      hadoopConf.reloadConfiguration();
    }
    conf = new YarnConfiguration(hadoopConf);

    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();
    numContainersToWaitFor = Integer.parseInt(cli.getOptionValue("nodes"));
    memory = Integer.parseInt(cli.getOptionValue("memory", "512"));
    port = Integer.parseInt(cli.getOptionValue("port"));
    nextPort = port;

    SecureRandom random = new SecureRandom();
    this.randomStopKey = new BigInteger(130, random).toString(32);

    this.inetAddresses = getMyInetAddresses();
  }

  protected String getMyInetAddresses() {
    Set<String> ipAddrs = new HashSet<String>();
    try {
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements()) {
        NetworkInterface next = ifaces.nextElement();
        for (InterfaceAddress addr : next.getInterfaceAddresses()) {
          InetAddress inetAddr = addr.getAddress();
          String ipAddr = inetAddr.getHostAddress();
          if (ipAddr != null && ipAddr.length() > 0) {
            // todo: use a regex
            if (!"127.0.0.1".equals(ipAddr) && (ipAddr.indexOf(".") != -1 && ipAddr.indexOf(":") == -1))
              ipAddrs.add(ipAddr);
          }
        }
      }
    } catch (Exception exc) {
      throw new RuntimeException("Failed to get address(es) of SolrMaster node due to: " + exc, exc);
    }
    StringBuilder addrs = new StringBuilder();
    for (String ip : ipAddrs) {
      if (addrs.length() > 0)
        addrs.append(",");
      addrs.append(ip);
    }

    return addrs.toString();
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public void run() throws Exception {
    int virtualCores = Integer.parseInt(cli.getOptionValue("virtualCores", "1"));

    AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
    rmClient.init(getConfiguration());
    rmClient.start();

    // Register with ResourceManager
    rmClient.registerApplicationMaster("", 0, "");

    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    capability.setVirtualCores(virtualCores);

    // Make container requests to ResourceManager
    for (int i = 0; i < numContainersToWaitFor; ++i)
      rmClient.addContainerRequest(new ContainerRequest(capability, null, null, priority));

    log.info("Waiting for " + numContainersToWaitFor + " containers to finish");
    while (!doneWithContainers())
      Thread.sleep(10000);

    log.info("SolrMaster application shutdown.");

    // Un-register with ResourceManager
    try {
      rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    } catch (Exception exc) {
      // safe to ignore ... this usually fails anyway
    }
  }

  public synchronized boolean doneWithContainers() {
    return isShutdown || numContainersToWaitFor <= 0;
  }

  public synchronized void onContainersAllocated(List<Container> containers) {
    String zkHost = cli.getOptionValue("zkHost");
    String solrArchive = cli.getOptionValue("solr");
    String hdfsHome = cli.getOptionValue("hdfs_home");

    Path pathToRes = new Path(solrArchive);
    FileStatus jarStat = null;
    try {
      jarStat = FileSystem.get(conf).getFileStatus(pathToRes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LocalResource solrPackageRes = Records.newRecord(LocalResource.class);
    solrPackageRes.setResource(ConverterUtils.getYarnUrlFromPath(pathToRes));
    solrPackageRes.setSize(jarStat.getLen());
    solrPackageRes.setTimestamp(jarStat.getModificationTime());
    solrPackageRes.setType(LocalResourceType.ARCHIVE);
    solrPackageRes.setVisibility(LocalResourceVisibility.APPLICATION);

    Map<String, LocalResource> localResourcesMap = new HashMap<String, LocalResource>();
    localResourcesMap.put("solr", solrPackageRes);

    String acceptShutdownFrom = "-Dyarn.acceptShutdownFrom=" + inetAddresses;

    log.info("Using " + acceptShutdownFrom);

    String dasha = "";
    if (hdfsHome != null) {
      dasha += " -a '-Dsolr.hdfs.home=" + hdfsHome + " -Dsolr.directoryFactory=HdfsDirectoryFactory -Dsolr.lock.type=hdfs %s'";
    } else {
      dasha += "-a '%s'";
    }

    dasha = String.format(dasha, acceptShutdownFrom);

    String command = "/bin/bash ./solr/bin/solr -f -c -p %d -k %s -m " + memory + "m -z " + zkHost + dasha + " -V";
    for (Container container : containers) {
      ContainerId containerId = container.getId();

      // increment the port if running on the same host
      int jettyPort = nextPort++;
      String jettyHost = container.getNodeId().getHost();
      Set<Integer> portsOnHost = solrHosts.get(jettyHost);
      if (portsOnHost == null) {
        portsOnHost = new HashSet<Integer>();
        solrHosts.put(jettyHost, portsOnHost);
      }
      portsOnHost.add(jettyPort);
      log.info("Added port " + jettyPort + " to host: " + jettyHost);

      try {
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setLocalResources(localResourcesMap);

        String cmd = String.format(command, jettyPort, randomStopKey);
        log.info("\n\nRunning command: " + cmd);

        ctx.setCommands(Collections.singletonList(
                cmd + " >" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout 2>&1"
        ));
        log.info("Launching container " + containerId);
        nmClient.startContainer(container, ctx);
      } catch (Exception exc) {
        log.error("Failed to start container to run Solr on port " + jettyPort + " due to: " + exc, exc);
      }
    }
  }

  public void onContainersCompleted(List<ContainerStatus> containerStatuses) {

    log.info("\n\n\n onContainersCompleted(" + containerStatuses + ") \n\n\n");

    for (ContainerStatus status : containerStatuses) {
      log.info("Completed container " + status.getContainerId());
      synchronized (this) {
        numContainersToWaitFor--;
      }
    }
  }

  public void onShutdownRequest() {

    log.info("\n\n\n onShutdownRequest ... shutting down: " + solrHosts + " \n\n\n");

    for (String host : solrHosts.keySet()) {
      for (Integer port : solrHosts.get(host)) {
        stopJettyOnHost(host, port, randomStopKey);
      }
    }

    isShutdown = true;

  }

  protected void stopJettyOnHost(String host, int jettyPort, String stopKey) {
    String commandUrl = "http://" + host + ":" + jettyPort + "/shutdown?token=" + stopKey;
    log.info("Sending stop command to " + commandUrl);
    HttpMethod method = null;
    try {
      HttpClient client = new HttpClient();
      method = new GetMethod(commandUrl);
      client.executeMethod(method);
    } catch (Exception exc) {
      log.warn("Failed to send shutdown command to " + commandUrl + " due to: " + exc, exc);
    } finally {
      method.releaseConnection();
    }
  }

  public void onNodesUpdated(List<NodeReport> nodeReports) {

    log.info("\n\n\n onNodesUpdated \n\n\n");
    for (NodeReport report : nodeReports) {
      log.info("Node: " + report);
    }

  }

  public float getProgress() {
    return 0;
  }

  public void onError(Throwable throwable) {
    log.info("\n\n\n BEG onError \n\n\n");

    log.error(throwable);

    log.info("\n\n\n END onError \n\n\n");

  }

  private static Options getOptions() {
    Options options = new Options();
    Option[] opts = getSolrMasterOptions();
    for (int i = 0; i < opts.length; i++)
      options.addOption(opts[i]);
    return options;
  }

  public static Option[] getSolrMasterOptions() {
    return new Option[]{
            OptionBuilder
                    .withArgName("HOST")
                    .hasArg()
                    .isRequired(true)
                    .withDescription("Address of the Zookeeper ensemble; defaults to: localhost:2181")
                    .create("zkHost"),
            OptionBuilder
                    .withArgName("ARCHIVE")
                    .hasArg()
                    .isRequired(true)
                    .withDescription("tgz file containing a Solr distribution.")
                    .create("solr"),
            OptionBuilder
                    .withArgName("PORT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Solr port; default is 8983")
                    .create("port"),
            OptionBuilder
                    .withArgName("PATH")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Solr HDFS home directory; if provided, Solr will store indexes in HDFS")
                    .create("hdfs_home"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Memory (mb) to allocate to each Solr node; default is 512M")
                    .create("memory"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Virtual cores to allocate to each Solr node; default is 1")
                    .create("virtualCores"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(true)
                    .withDescription("Number of Solr nodes to deploy; default is 1")
                    .create("nodes"),
            OptionBuilder
                    .withArgName("PATH")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("YARN ResourceManager address")
                    .create("conf")
    };
  }
}
