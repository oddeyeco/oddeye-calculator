/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calculator;

import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.DateTime;
import org.apache.log4j.PropertyConfigurator;
import org.hbase.async.BinaryComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.QualifierFilter;
import org.hbase.async.ScanFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

//import org.apache.samoa.core.Processor;
//import org.apache.samoa.topology.Stream;
//import org.apache.samoa.topology.TopologyBuilder;
/**
 *
 * @author vahan
 */
public class MainClass {

    protected static org.hbase.async.HBaseClient client;
    protected static TSDB tsdb;
    private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class);
    private static byte[] key;
    private static final byte[] family = "d".getBytes();
    private static final Calendar CalendarObj = Calendar.getInstance();
    private static int a_numThreads;
    private static ExecutorService executor;

    public static void main(String[] args) throws Exception {

        String argskey = "";
        String time = "1h-ago";
        String end_time = "now";
        Short daycount = 1;

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
            if (argskey.equals("-st")) {
                time = s;
            }
            if (argskey.equals("-et")) {
                end_time = s;
            }
            if (argskey.equals("-l")) {
                logconfig = s;
            }
            if (argskey.equals("-c")) {
                configfile = s;
            }
            if (argskey.equals("-d")) {
                daycount = Short.parseShort(s);
            }
            argskey = s;
        }

        path = Paths.get(configfile);

        if (Files.notExists(path)) {
            throw new FileNotFoundException(path.toString());
        }

        path = Paths.get(logconfig);

        if (Files.notExists(path)) {
            throw new FileNotFoundException(path.toString());
        }

        final Calendar EndCalendarObj = Calendar.getInstance();
        final Calendar StartCalendarObj = Calendar.getInstance();
        StartCalendarObj.setTime(new Date(DateTime.parseDateTimeString(time, null)));
        EndCalendarObj.setTime(new Date(DateTime.parseDateTimeString(end_time, null)));
        EndCalendarObj.set(Calendar.MILLISECOND, 0);
        EndCalendarObj.set(Calendar.SECOND, 0);
        EndCalendarObj.set(Calendar.MINUTE, 0);
        EndCalendarObj.add(Calendar.MILLISECOND, -1);
        StartCalendarObj.set(Calendar.MILLISECOND, 0);
        StartCalendarObj.set(Calendar.SECOND, 0);
        StartCalendarObj.set(Calendar.MINUTE, 0);

        PropertyConfigurator.configure(logconfig);
        Yaml yaml = new Yaml();
        Map<String, Object> conf = (Map<String, Object>) yaml.load(new InputStreamReader(new FileInputStream(configfile)));

        if (EndCalendarObj.getTimeInMillis() < StartCalendarObj.getTimeInMillis()) {
            throw new Exception("End time " + EndCalendarObj.getTime() + " must be greater than the start time " + StartCalendarObj.getTime());
        }

        LOGGER.warn("Start calculate From " + StartCalendarObj.getTime() + " to " + EndCalendarObj.getTime());
        String current = new java.io.File(".").getCanonicalPath();
        LOGGER.debug("Current dir:" + current);

        String quorum = String.valueOf(conf.get("zkHosts"));
        client = new org.hbase.async.HBaseClient(quorum);

        String metatable = String.valueOf(conf.get("metatable"));

        net.opentsdb.utils.Config openTsdbConfig = new net.opentsdb.utils.Config(true);
        openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
        openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));
        tsdb = new TSDB(
                client,
                openTsdbConfig);

//        OddeeyMetricMeta mtrsc;
        OddeeyMetricMetaList mtrscList;
        try {
            mtrscList = new OddeeyMetricMetaList(tsdb, metatable.getBytes());
        } catch (Exception ex) {
            mtrscList = new OddeeyMetricMetaList();
        }

//        final Map<String, String> list = new HashMap<>();
//        final Map<String, String> cluster = new HashMap<>();
//        final Map<String, String> host = new HashMap<>();
//        for (OddeeyMetricMeta mtrsc : mtrscList.values()) {
//            list.put(mtrsc.getTags().get("UUID").getValue(), mtrsc.getTags().get("UUID").getValue());
//            cluster.put(mtrsc.getTags().get("cluster").getValue(), mtrsc.getTags().get("cluster").getValue());
//            host.put(mtrsc.getTags().get("host").getValue(), mtrsc.getTags().get("host").getValue());
//
//        }
//        System.out.println(list.size());
//        System.out.println(cluster.size());
//        System.out.println(cluster);
//        System.out.println(host.size());
        int i = 0;
        long Allstarttime = System.currentTimeMillis();
        int threadNumber = 0;
        try {
            executor = Executors.newFixedThreadPool(10);

            // now create an object to consume the messages
            //
            

            for (OddeeyMetricMeta mtrsc : mtrscList.values()) {

                executor.submit(new CalculateRuleTask(threadNumber,mtrsc,StartCalendarObj,EndCalendarObj,metatable));
                threadNumber++;
//            i++;
            }
            
            executor.shutdown();
//            final boolean done = executor.awaitTermination(1, TimeUnit.MINUTES);
            while (!executor.awaitTermination(10, TimeUnit.MINUTES))
            {
                Thread.sleep(1);
            }            
        } catch (Exception e) {
            throw new Exception(e);
        }

        long Allendtime = System.currentTimeMillis() - Allstarttime;
        LOGGER.warn(threadNumber + " of " + mtrscList.size() + " done in " + Allendtime / 1000 + " s");
        client.flush();
        LOGGER.warn("Flush all");
        LOGGER.warn("mtrscList.size:" + mtrscList.size());

        System.exit(0);
    }

}
