/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calculator;

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

    private static Logger LOGGER = LoggerFactory.getLogger(MainClass.class);

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

        String metatable = String.valueOf(conf.get("oddeyerulestable"));

        net.opentsdb.utils.Config openTsdbConfig = new net.opentsdb.utils.Config(true);
        openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
        openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
        openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));
        tsdb = new TSDB(
                client,
                openTsdbConfig);

        List<String> Metrics = tsdb.suggestMetrics("", Integer.MAX_VALUE);

        LOGGER.info("Metrics Count = " + Metrics.size());

        final TSQuery tsquery = new TSQuery();

        tsquery.setStart(Long.toString(StartCalendarObj.getTimeInMillis()));
        tsquery.setEnd(Long.toString(EndCalendarObj.getTimeInMillis()));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>(1);
        final Map<String, String> tags = new HashMap<>();
        final Map<String, SeekableView> datalist = new TreeMap<>();
        byte[] key;
        byte[] family;
        byte[] qualifier;
        byte[] value;
        tags.put("host", "*");
        tags.put("UUID", "*");
        TagVFilter.mapToFilters(tags, filters, true);

        for (String metric : Metrics) {
//            metric = "cpu_user";
            final TSSubQuery sub_query_dev = new TSSubQuery();
//            metric = "sys_load_1";
            sub_query_dev.setMetric(metric);
            sub_query_dev.setAggregator("dev");
            sub_query_dev.setFilters(filters);
            sub_query_dev.setDownsample("1h-dev");
            sub_query_dev.setIndex(0);
            sub_queries.add(sub_query_dev);

            final TSSubQuery sub_query_avg = new TSSubQuery();
            sub_query_avg.setMetric(metric);
            sub_query_avg.setAggregator("avg");

            sub_query_avg.setFilters(filters);
            sub_query_avg.setDownsample("1h-avg");
            sub_queries.add(sub_query_avg);

            final TSSubQuery sub_query_max = new TSSubQuery();
            sub_query_max.setMetric(metric);
            sub_query_max.setAggregator("max");

            sub_query_max.setFilters(filters);
            sub_query_max.setDownsample("1h-max");
            sub_queries.add(sub_query_max);
            final TSSubQuery sub_query_min = new TSSubQuery();
            sub_query_min.setMetric(metric);
            sub_query_min.setAggregator("min");

            sub_query_min.setFilters(filters);
            sub_query_min.setDownsample("1h-min");
            sub_queries.add(sub_query_min);
//            break;
        }
        tags.clear();
        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);
        Map<String, String> Tagmap;

        final Calendar CalendarObj = Calendar.getInstance();
        long starttime;
        long endtime;
        // create some arrays for storing the results and the async calls
        final int nqueries = tsdbqueries.length;

        for (int i = 0; i < nqueries; i++) {
            starttime = System.currentTimeMillis();
            LOGGER.info("Start tsdbqueries");
            final DataPoints[] series = tsdbqueries[i].run();
            endtime = System.currentTimeMillis() - starttime;
            LOGGER.info("Finish tsdbqueries " + i + "of "+nqueries+" in " + endtime + " ms");
            for (final DataPoints datapoints : series) {
                final SeekableView Datalist = datapoints.iterator();
                Tagmap = null;
                try {
                    Tagmap = datapoints.getTags();
                } catch (Exception e) {
                    LOGGER.warn("Invalid tags");
                    continue;
                }

//                datalist.put(Tagmap.get("host")+"|"+datapoints.getQueryIndex()  , Datalist);                
                while (Datalist.hasNext()) {
                    final DataPoint Point = Datalist.next();
                    CalendarObj.setTimeInMillis(Point.timestamp());
                    qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID")));
                    qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));
                    family = sub_queries.get(datapoints.getQueryIndex()).downsamplingSpecification().getFunction().toString().getBytes();
                    key = ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                    value = ByteBuffer.allocate(8).putDouble(Point.doubleValue()).array();

                    final PutRequest putrule = new PutRequest(metatable.getBytes(), key, family, qualifier, value);
                    client.put(putrule);

                }
            }
            endtime = System.currentTimeMillis() - starttime;
            LOGGER.info(i + " of " + nqueries + " done in " + endtime + " ms");
        }

        client.flush();
        LOGGER.warn("Flush all");
        
        System.exit(0);
    }

}
