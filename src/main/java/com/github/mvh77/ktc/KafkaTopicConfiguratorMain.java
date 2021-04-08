package com.github.mvh77.ktc;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.kohsuke.args4j.OptionHandlerFilter.ALL;

public class KafkaTopicConfiguratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicConfiguratorMain.class);

    @Option(name="-bootstrap", usage = "kafka bootstrap servers, in the form host1:port1,host2:port2,...", required = true)
    private String bootstrap;

    @Option(name = "-definitions", usage = "topic definition files, in the form config1.yml,config2.yml,...", required = true)
    private String definitions;

    @Option(name = "-extraProperties", usage = "extra .properties files for configuring the client, in the form config1.properties,config2.properties,...")
    private String extraProperties;

    @Option(name = "-dryRun", usage = "don't run any of the updates, just print the current topics and the updates to execute")
    private boolean dryRun = false;

    @Option(name = "-removeTopics", usage = "remove topics missing from the definition files")
    private boolean removeTopics = false;

    @Option(name = "-noReplication", usage = "don't respect replication numbers for local testing purposes")
    private boolean noReplication = false;

    public void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line, you'll get this exception. this will report an error message.
            System.err.println(e.getMessage());
            System.err.println("java KafkaTopicConfiguratorMain [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();

            // print option sample. This is useful some time
            System.err.println("  Example: java KafkaTopicConfiguratorMain" + parser.printExample(ALL));
            return;
        }
        new KafkaTopicConfigurator().execute(bootstrap, definitions, extraProperties, dryRun, removeTopics, noReplication);
    }

    public static void main(String[] args) {
        new KafkaTopicConfiguratorMain().doMain(args);
    }

}
