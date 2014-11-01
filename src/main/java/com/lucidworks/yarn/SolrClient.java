package com.lucidworks.yarn;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import org.apache.log4j.Logger;

/**
 * Client for submitting the SolrCloud application to YARN.
 */
public class SolrClient {

  public static Logger log = Logger.getLogger(SolrClient.class);

  Configuration conf = new YarnConfiguration();

  public void run(CommandLine cli) throws Exception {

    YarnConfiguration conf = new YarnConfiguration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    logYarnDiagnostics(yarnClient);

    YarnClientApplication app = yarnClient.createApplication();

    String hdfsHome = "";
    String hdfsHomeOption = cli.getOptionValue("hdfs_home");
    if (hdfsHomeOption != null)
      hdfsHome = " -hdfs_home="+hdfsHomeOption;

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    amContainer.setCommands(
      Collections.singletonList(
        "$JAVA_HOME/bin/java" +
                " -Xmx128M" +
                " com.lucidworks.yarn.SolrMaster" +
                " -port=" + cli.getOptionValue("port", "8983") +
                " -zkHost=" + cli.getOptionValue("zkHost", "localhost:2181") +
                " -nodes=" + Integer.parseInt(cli.getOptionValue("nodes", "1")) +
                " -memory=" + cli.getOptionValue("memory", "512") +
                " -virtualCores=" + cli.getOptionValue("virtualCores", "2") +
                " -solr=" + cli.getOptionValue("solr") +
                hdfsHome +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
      )
    );

    // Setup jar for ApplicationMaster
    Map<String,LocalResource> localResourcesMap = new HashMap<String,LocalResource>();

    LocalResource solrAppJarLocalResource = Records.newRecord(LocalResource.class);
    setupSolrAppJar(new Path(cli.getOptionValue("jar")), solrAppJarLocalResource);
    localResourcesMap.put("app.jar", solrAppJarLocalResource);

    amContainer.setLocalResources(localResourcesMap);

    // Setup CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(128);
    capability.setVirtualCores(1);

    // Finally, set-up ApplicationSubmissionContext for the application
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName(cli.getOptionValue("name", "SolrCloud"));
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue(cli.getOptionValue("queue", "default"));

    // Submit application
    ApplicationId appId = appContext.getApplicationId();
    log.info("Submitting application " + appId);
    yarnClient.submitApplication(appContext);

    // Poll status untile we're running or failed
    ApplicationReport appReport = yarnClient.getApplicationReport(appId);
    YarnApplicationState appState = appReport.getYarnApplicationState();
    while (appState != YarnApplicationState.RUNNING &&
            appState != YarnApplicationState.KILLED &&
            appState != YarnApplicationState.FAILED) {
      Thread.sleep(10000);
      appReport = yarnClient.getApplicationReport(appId);
      appState = appReport.getYarnApplicationState();
    }

    log.info("Solr (" + appId + ") is " + appState);
  }

  protected void setupSolrAppJar(Path jarPath, LocalResource solrMasterJar) throws IOException {
    FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
    solrMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    solrMasterJar.setSize(jarStat.getLen());
    solrMasterJar.setTimestamp(jarStat.getModificationTime());
    solrMasterJar.setType(LocalResourceType.FILE);
    solrMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
  }

  protected void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
              c.trim());
    }
    Apps.addToEnvironment(appMasterEnv,
            Environment.CLASSPATH.name(),
            Environment.PWD.$() + File.separator + "*");
  }

  protected void logYarnDiagnostics(YarnClient yarnClient) throws IOException, YarnException {
    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    log.info("Got Cluster metric info from ASM" + ", numNodeManagers="
            + clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports();
    log.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      log.info("Got node report from ASM for" + ", nodeId="
              + node.getNodeId() + ", nodeAddress"
              + node.getHttpAddress() + ", nodeRackName"
              + node.getRackName() + ", nodeNumContainers"
              + node.getNumContainers());
    }

    QueueInfo queueInfo = yarnClient.getQueueInfo("default");
    log.info("Queue info" + ", queueName=" + queueInfo.getQueueName()
            + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
            + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
            + ", queueApplicationCount="
            + queueInfo.getApplications().size()
            + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        log.info("User ACL Info for Queue" + ", queueName="
                + aclInfo.getQueueName() + ", userAcl="
                + userAcl.name());
      }
    }
  }


  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args!");
      displayUsage(System.err);
      System.exit(1);
    }

    CommandLine cli = processCommandLineArgs(SolrClient.class.getName(), getOptions(), args);
    SolrClient c = new SolrClient();
    c.run(cli);
  }

  public static void displayUsage(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(SolrClient.class.getName(), getOptions());
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] opts = getSolrClientOptions();
    for (int i = 0; i < opts.length; i++)
      options.addOption(opts[i]);
    return options;
  }

  public static Option[] getSolrClientOptions() {
    return new Option[] {
      OptionBuilder
              .withArgName("NAME")
              .hasArg()
              .isRequired(false)
              .withDescription("Application name; defaults to: SolrCloud")
              .create("name"),
      OptionBuilder
              .withArgName("QUEUE")
              .hasArg()
              .isRequired(false)
              .withDescription("YARN queue; default is default")
              .create("queue"),
      OptionBuilder
              .withArgName("HOST")
              .hasArg()
              .isRequired(false)
              .withDescription("Address of the Zookeeper ensemble; defaults to: localhost:2181")
              .create("zkHost"),
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
              .withArgName("JAR")
              .hasArg()
              .isRequired(true)
              .withDescription("JAR file containing the SolrCloud YARN Application Master")
              .create("jar"),
      OptionBuilder
              .withArgName("ARCHIVE")
              .hasArg()
              .isRequired(true)
              .withDescription("tgz file containing a Solr distribution.")
              .create("solr"),
      OptionBuilder
              .withArgName("INT")
              .hasArg()
              .isRequired(false)
              .withDescription("Number of Solr nodes to deploy; default is 1")
              .create("nodes"),
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
              .create("virtualCores")

    };
  }

  /**
   * Parses the command-line arguments passed by the user.
   */
  public static CommandLine processCommandLineArgs(String app, Options options, String[] args) {
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(app, options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(app, options);
      System.exit(0);
    }

    return cli;
  }
}
