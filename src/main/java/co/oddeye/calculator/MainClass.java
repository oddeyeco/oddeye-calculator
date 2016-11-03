/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calculator;

import java.io.FileInputStream;
import java.io.InputStreamReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Calendar;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 *
 * @author vahan
 */
public class MainClass {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    protected static org.hbase.async.HBaseClient client;
    private static TSDB tsdb;   
    static final Logger log = LoggerFactory.getLogger(MainClass.class);

    public MainClass(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, a_numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new CalcByKafkaTask(stream, threadNumber, tsdb, client));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws Exception {
        String argskey = "";
        int time = 300000;
        int threads = 1;
        String configfile = "config.yaml";
        String logconfig = "log4j.properties";

        Path path = Paths.get(configfile);        
        
        if (Files.notExists(path)) {
            configfile = "target/classes/config.yaml";
        }

        path = Paths.get(logconfig);

        if (Files.notExists(path)) {
            logconfig = "target/classes/log4j.properties";
        }
        
        for (String s : args) {
            if (argskey.equals("-t")) {
                time = Integer.parseInt(s);
            }
            if (argskey.equals("-p")) {
                threads = Integer.parseInt(s);
            }
            if (argskey.equals("-l")) {
                logconfig = s;
            }
            if (argskey.equals("-c")) {
                configfile = s;
            }            
            argskey = s;
        }

        PropertyConfigurator.configure(logconfig);
        Yaml yaml = new Yaml();
        Map<String, Object> conf = (Map<String, Object>) yaml.load(new InputStreamReader(new FileInputStream(configfile)));
        

        String quorum = String.valueOf(conf.get("zkHosts"));
        client = new org.hbase.async.HBaseClient(quorum);        
        
        Config openTsdbConfig = new net.opentsdb.utils.Config(true);
        openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
        openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));
        tsdb = new TSDB(
                client,
                openTsdbConfig);

        String zooKeeper = String.valueOf(conf.get("kafka.zkHosts"));
        String groupId = String.valueOf(conf.get("kafka.groupId"));
        String topic = String.valueOf(conf.get("kafka.topic"));

        final Calendar CalendarObj = Calendar.getInstance();

        MainClass example = new MainClass(zooKeeper, groupId, topic);
        example.run(threads);
        if (time == -1) {
            while (true) {

            }
        } else {
            try {
                Thread.sleep(time);
            } catch (InterruptedException ie) {

            }
            example.shutdown();
        }
        System.exit(0);
    }

}
