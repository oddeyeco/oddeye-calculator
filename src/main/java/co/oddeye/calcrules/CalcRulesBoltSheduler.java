/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calcrules;

import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonParser;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CalcRulesBoltSheduler {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CalcRulesBoltSheduler.class);
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private final byte[] metatable;
    private OddeeyMetricMetaList MetricMetaList;
    private Calendar CalendarObjRules;
    private boolean needsave;
    private long starttime;
    private long endtime;
    private byte[] key;
    private final byte[] family = "d".getBytes();

    private JsonParser parser = null;


    /**
     *
     */
    public CalcRulesBoltSheduler() {
        this.metatable = "oddeye-meta".getBytes();
    }

    public void prepare() {
        LOGGER.warn("DoPrepare Calc Rules ");

        parser = new JsonParser();

        try {
            String quorum = "zk00.oddeye.co:2181,zk01.oddeye.co:2181,zk02.oddeye.co:2181";
            openTsdbConfig = new net.opentsdb.utils.Config(true);

            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(true));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(false));

            openTsdbConfig.overrideConfig("tsd.core.enable_api", String.valueOf(false));
            openTsdbConfig.overrideConfig("tsd.core.enable_ui", String.valueOf(false));

            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", "oddeye-data");
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", "oddeye-data-uid");

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", String.valueOf(4096));
            globalFunctions.getTSDB(openTsdbConfig, clientconf);
            CalendarObjRules = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

//            this.metatable = String.valueOf(conf.get("metatable")).getBytes();
            try {
                LOGGER.warn("Start read meta in hbase");
//                MetricMetaList = new OddeeyMetricMetaList();
                MetricMetaList = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable);
                LOGGER.warn("End read meta in hbase");
            } catch (Exception ex) {
                MetricMetaList = new OddeeyMetricMetaList();
            }

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");
    }

    public void execute() {
        try {
            final HBaseClient client = globalFunctions.getTSDB(openTsdbConfig, clientconf).getClient();
            final TSDB tsdb = globalFunctions.getTSDB(openTsdbConfig, clientconf);
            ArrayList<ArrayList<KeyValue>> rows;
            starttime = System.currentTimeMillis();

            long metriccount = 0;
            long calmetriccount = 0;
            CalendarObjRules = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            CalendarObjRules.add(Calendar.DATE, -1);
            CalendarObjRules.add(Calendar.HOUR, 2);
            Map<String, Map<Integer, OddeeyMetricMeta>> namemap = new HashMap<>();
            for (Map.Entry<Integer, OddeeyMetricMeta> meta : MetricMetaList.entrySet()) {
                if ((!meta.getValue().isSpecial()) && (meta.getValue().getLasttime() > (starttime - (1000 * 60 * 60)))) {
                    if (!namemap.containsKey(meta.getValue().getName())) {
                        namemap.put(meta.getValue().getName(), new HashMap<>());
                    }
                    namemap.get(meta.getValue().getName()).put(meta.hashCode(), meta.getValue());
                }
            }
            Map<String, Map<String, DescriptiveStatistics>> statslist = new HashMap<>();
            class QueriesCB implements Callback<Object, ArrayList<DataPoints[]>> {

                private final long enddate;
                private final long startdate;

                public QueriesCB(long e_date, long s_date) {
                    enddate = e_date;
                    startdate = s_date;
                }

                @Override
                public Object call(final ArrayList<DataPoints[]> query_results)
                        throws Exception {
                    double R_value;
                    byte[] time_key;

                    for (DataPoints[] series : query_results) {
                        for (final DataPoints datapoints : series) {
                            final SeekableView Datalist = datapoints.iterator();
                            System.out.println(datapoints.getTags() + " - " + datapoints.metricName());
                            OddeeyMetricMeta tmpmetric = new OddeeyMetricMeta(datapoints.metricName(), datapoints.getTags(), tsdb);
                            while (Datalist.hasNext()) {
                                final DataPoint Point = Datalist.next();
                                if ((Point.timestamp() > enddate) || (Point.timestamp() < startdate)) {
                                    continue;
                                }
                                final Calendar CalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                                CalendarObj.setTimeInMillis(Point.timestamp());
                                time_key = ByteBuffer.allocate(6).putShort((short) CalendarObj.get(Calendar.YEAR)).putShort((short) CalendarObj.get(Calendar.DAY_OF_YEAR)).putShort((short) CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                                R_value = Point.doubleValue();

                                Map<String, DescriptiveStatistics> Metricstats = statslist.get(Hex.encodeHexString(tmpmetric.getKey()));
                                if (Metricstats == null) {
                                    Metricstats = new HashMap<>();
                                    statslist.put(Hex.encodeHexString(tmpmetric.getKey()), Metricstats);
                                }
                                DescriptiveStatistics stats = Metricstats.get(Hex.encodeHexString(time_key));
                                if (stats == null) {
                                    stats = new DescriptiveStatistics();
                                    Metricstats.put(Hex.encodeHexString(time_key), stats);
                                }
                                stats.addValue(R_value);
                            }
                        }
                    }

                    return null;
                }
            }

            System.out.println(namemap.size());

            final Calendar CalObjRules = (Calendar) CalendarObjRules.clone();
            CalObjRules.set(Calendar.MILLISECOND, 0);
            CalObjRules.set(Calendar.SECOND, 0);
            CalObjRules.set(Calendar.MINUTE, 0);
            final Calendar CalObjRulesEnd = (Calendar) CalObjRules.clone();
            CalObjRulesEnd.add(Calendar.HOUR, 1);
            CalObjRulesEnd.add(Calendar.MILLISECOND, -1);
            for (Map.Entry<String, Map<Integer, OddeeyMetricMeta>> nameSet : namemap.entrySet()) {
                calmetriccount++;
                String name = nameSet.getKey();
                final TSQuery tsquery = new TSQuery();
                tsquery.setStart(Long.toString(CalObjRules.getTimeInMillis()));
                tsquery.setEnd(Long.toString(CalObjRulesEnd.getTimeInMillis()));
                final ArrayList<TSSubQuery> sub_queries = new ArrayList<>();
                final TSSubQuery sub_query = new TSSubQuery();
                sub_query.setMetric(name);
                sub_query.setAggregator("none");
                sub_queries.add(sub_query);

                tsquery.setQueries(sub_queries);
                tsquery.validateAndSetQuery();
                Query[] tsdbqueries = tsquery.buildQueries(tsdb);

                final int nqueries = tsdbqueries.length;
                final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>();
                for (int nq = 0; nq < nqueries; nq++) {
                    deferreds.add(tsdbqueries[nq].runAsync());
                }
//                Deferred.groupInOrder(deferreds).addCallback(new QueriesCB(CalObjRulesEnd.getTimeInMillis(), CalObjRules.getTimeInMillis()));
                Deferred.groupInOrder(deferreds).addCallback(new QueriesCB(CalObjRulesEnd.getTimeInMillis(), CalObjRules.getTimeInMillis())).join();
                System.out.println("calmetriccount " + calmetriccount + " from " + namemap.size() + "  in " + ((System.currentTimeMillis() - starttime) / 1000 / 60) + " min");                                
            }
            System.out.println("finish " + ((System.currentTimeMillis() - starttime) / 1000 / 60) + " " + metriccount + " calmetriccount " + calmetriccount);
            for (Map.Entry<String, Map<String, DescriptiveStatistics>> stat : statslist.entrySet()) {
                if (stat.getKey().charAt(0) == '@') {
                    key = stat.getKey().getBytes();
                } else {
                    key = Hex.decodeHex(stat.getKey());
                }

                byte[][] qualifiers = new byte[stat.getValue().size()][];
                byte[][] values = new byte[stat.getValue().size()][];
                int index = 0;
                for (Map.Entry<String, DescriptiveStatistics> satdata : stat.getValue().entrySet()) {
                    byte[] time_key = Hex.decodeHex(satdata.getKey());
                    MetriccheckRule RuleItem = new MetriccheckRule(time_key);
                    RuleItem.update("avg", satdata.getValue().getMean());
                    RuleItem.update("dev", satdata.getValue().getStandardDeviation());
                    RuleItem.update("min", satdata.getValue().getMin());
                    RuleItem.update("max", satdata.getValue().getMax());
                    RuleItem.setHasNotData(false);
                    qualifiers[index] = time_key;
                    values[index] = RuleItem.getValues();
                    index++;
                }
                System.out.println(stat.getKey());
                if (qualifiers.length > 0) {
                    try {
                        PutRequest putvalue = new PutRequest(metatable, key, family, qualifiers, values);
                        globalFunctions.getClient(clientconf).put(putvalue);
                    } catch (Exception e) {
                        LOGGER.error("catch In Multi qualifiers stackTrace: " + globalFunctions.stackTrace(e));

                    }
                }
            }
            tsdb.shutdown().join();
            client.shutdown().join();            
        } catch (Exception ex) {
            LOGGER.warn(globalFunctions.stackTrace(ex));
        }

    }

}
