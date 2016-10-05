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
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.PropertyConfigurator;
import org.hbase.async.PutRequest;
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
    private static TSDB tsdb;

    private static final DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:m:s");

    private static final Logger LOGGER = LoggerFactory.getLogger(MainClass.class);
    private static byte[] key;
    private static final byte[] family = "d".getBytes();

    public static void main(String[] args) throws Exception {

        String argskey = "";
        String time = "1h-ago";
        String configfile = "target/classes/config.yaml";

        String logconfig = "target/classes/log4j.properties";
        for (String s : args) {
            if (argskey.equals("-t")) {
                time = s;
            }
            if (argskey.equals("-l")) {
                logconfig = s;
            }
            if (argskey.equals("-c")) {
                configfile = s;
            }
            argskey = s;
        }

        final Calendar EndCalendarObj = Calendar.getInstance();
        final Calendar StartCalendarObj = Calendar.getInstance();
        StartCalendarObj.setTime(new Date(DateTime.parseDateTimeString(time, null)));
        EndCalendarObj.set(Calendar.MILLISECOND, 0);
        EndCalendarObj.set(Calendar.SECOND, 0);
        EndCalendarObj.set(Calendar.MINUTE, 0);
        EndCalendarObj.add(Calendar.MILLISECOND, -1);
        StartCalendarObj.set(Calendar.MILLISECOND, 0);
        StartCalendarObj.set(Calendar.SECOND, 0);
        StartCalendarObj.set(Calendar.MINUTE, 0);

        PropertyConfigurator.configure(logconfig);

        String Filename = configfile;

        Yaml yaml = new Yaml();
        Map<String, Object> conf = (Map<String, Object>) yaml.load(new InputStreamReader(new FileInputStream(configfile)));

        LOGGER.info("Start calculate From " + StartCalendarObj.getTime() + " to " + EndCalendarObj.getTime());
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

        int i = 0;
        long Allstarttime = System.currentTimeMillis();
        for (OddeeyMetricMeta mtrsc : mtrscList) {
            long starttime = System.currentTimeMillis();
            mtrsc.CalculateRules(StartCalendarObj.getTimeInMillis(), EndCalendarObj.getTimeInMillis(), tsdb);
            key = mtrsc.getKey();
            byte[][] qualifiers;
            byte[][] values;
            ConcurrentMap<String, MetriccheckRule> rulesmap = mtrsc.getRulesMap();
            qualifiers = new byte[rulesmap.size()][];
            values = new byte[rulesmap.size()][];
            int index = 0;
            for (Map.Entry<String, MetriccheckRule> rule : rulesmap.entrySet()) {
                qualifiers[index] = rule.getValue().getKey();
                values[index] = rule.getValue().getValues();
                index++;
            }

            if (qualifiers.length > 0) {
                PutRequest putvalue = new PutRequest(metatable.getBytes(), key, family, qualifiers, values);
                client.put(putvalue);
            } else {
                PutRequest putvalue = new PutRequest(metatable.getBytes(), key, family, "n".getBytes(), key);
                client.put(putvalue);
            }
            i++;
            long endtime = System.currentTimeMillis() - starttime;
            LOGGER.warn(i + " of " + mtrscList.size() + " done in " + endtime + " ms");

        }
        long Allendtime = System.currentTimeMillis() - Allstarttime;
        LOGGER.warn(i + " of " + mtrscList.size() + " done in " + Allendtime/1000 + " s");

//        }
        client.flush();
        LOGGER.warn("Flush all");

        System.exit(0);
    }

}
