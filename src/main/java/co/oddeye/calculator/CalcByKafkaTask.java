/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calculator;

/**
 *
 * @author vahan
 */
import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import com.google.gson.Gson;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import net.opentsdb.core.TSDB;
import org.hbase.async.HBaseClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.stumbleupon.async.Deferred;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import kafka.message.MessageAndMetadata;
import net.opentsdb.core.DataPoints;
import org.hbase.async.PutRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalcByKafkaTask implements Runnable {

    static final Logger LOGGER = LoggerFactory.getLogger(CalcByKafkaTask.class);
    private final KafkaStream m_stream;
    private final int m_threadNumber;

    private final HBaseClient m_client;
    private final TSDB m_tsdb;
    private final JsonParser parser;
    private JsonArray jsonResult;    
    private MetriccheckRule Rule;    
    private final HashMap<Object, Object> tags = new HashMap<>();
    private long metrictime;
    private final Calendar StartCalendarObj = new Calendar.Builder().build();
    private final Calendar EndCalendarObj = new Calendar.Builder().build();
    private long starttime;
    private long endtime;
    private boolean needsave;

    public CalcByKafkaTask(KafkaStream a_stream, int a_threadNumber, TSDB tsdb, HBaseClient client) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        m_client = client;
        m_tsdb = tsdb;
        parser = new JsonParser();
    }

    @Override
    public void run() {
        final Calendar CalendarObj = Calendar.getInstance();
        final Calendar CalendarObjRules = Calendar.getInstance();

        NumberFormat formatter = new DecimalFormat("#0.0000");
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        JsonElement Metric;
        OddeeyMetricMeta mtrsc;
        OddeeyMetricMetaList mtrscList;
        byte[] metatable = "test_oddeye-meta".getBytes();
        try {
            mtrscList = new OddeeyMetricMetaList(m_tsdb, metatable);
        } catch (Exception ex) {
            mtrscList = new OddeeyMetricMetaList();
        }

        byte[] key;
        byte[] family = "d".getBytes();
        byte[] qualifier;
        byte[] value;
        Gson gson = new Gson();
        final Calendar RuleStart = Calendar.getInstance();
        final Calendar RuleEnd = Calendar.getInstance();

        RuleEnd.set(CalendarObj.get(Calendar.YEAR), CalendarObj.get(Calendar.MONTH), CalendarObj.get(Calendar.DAY_OF_MONTH), CalendarObj.get(Calendar.HOUR_OF_DAY), 0);
        RuleStart.set(CalendarObj.get(Calendar.YEAR), CalendarObj.get(Calendar.MONTH), CalendarObj.get(Calendar.DAY_OF_MONTH) - 1, CalendarObj.get(Calendar.HOUR_OF_DAY), 0);

        while (it.hasNext()) {            
            final MessageAndMetadata<byte[], byte[]> message = it.next();
            final String msg = new String(message.message());            

//            System.out.println("Offset:" + message.offset());
//            final String msg = "[{\"timestamp\":1475564807,\"metric\":\"mem_total\",\"value\":\"4085243904\",\"tags\":{\"cluster\":\"MouseflowEU\",\"host\":\"vpneu1.mouseflow.eu\",\"type\":\"system\",\"alert_level\":0,\"group\":\"vpn\",\"UUID\":\"da476957-6bfc-4fa0-acd0-fd0fde098fd4\"}}]";
            try {
                jsonResult = (JsonArray) this.parser.parse(msg);
                if (this.jsonResult != null) {
                    try {
                        if (this.jsonResult.size() > 0) {
                            LOGGER.info("Ready count: " + this.jsonResult.size());
                            CalendarObjRules.setTime(new Date());
                            CalendarObjRules.add(Calendar.HOUR, -1);

                            for (int i = 0; i < this.jsonResult.size(); i++) {
                                Metric = this.jsonResult.get(i);

                                if (Metric.getAsJsonObject().get("tags") != null) {
                                    if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                                        LOGGER.warn("host not exist in input " + msg);
                                    }
                                    if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                                        LOGGER.warn("type not exist in input " + msg);
                                    }
                                    if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("alert_level") == null) {
                                        LOGGER.warn("alert_level not exist in input " + msg);
                                    }
                                    if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                                        LOGGER.warn("group not exist in input " + msg);
                                    }
                                    if (Metric.getAsJsonObject().get("timestamp") == null) {
                                        LOGGER.warn("timestamp not exist in input " + msg);
                                    }

                                    if (Metric.getAsJsonObject().get("metric") == null) {
                                        LOGGER.warn("metric not exist in input " + msg);
                                    }
                                    if (Metric.getAsJsonObject().get("value") == null) {
                                        LOGGER.warn("value not exist in input " + msg);
                                    }
                                } else {
                                    LOGGER.warn("tags not exist in input " + msg);
                                }

                                CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                                mtrsc = new OddeeyMetricMeta(Metric, m_tsdb);
//                                if (!mtrsc.getName().equals("cpu_user")) {
//                                    continue;
//                                }
//                                if (!mtrsc.getTags().get("host").getValue().contains("cassa005")) {
//                                    continue;
//                                }

//                                metric_index = mtrscList.containsKey(mtrsc.hashCode());
                                if (mtrscList.containsKey(mtrsc.hashCode())) {
                                    mtrsc = mtrscList.get(mtrsc.hashCode());
                                }

                                metrictime = Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000;

                                CalendarObjRules.setTimeInMillis(metrictime);
                                CalendarObjRules.add(Calendar.HOUR, 1);

                                LOGGER.info(CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                needsave = false;
                                final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>();
                                for (int j = 0; j < 7; j++) {
                                    CalendarObjRules.add(Calendar.DATE, -1);
                                    try {
                                        Rule = mtrsc.getRule(CalendarObjRules, metatable, m_client);
                                    } catch (Exception ex) {
                                        LOGGER.warn("Rule exeption: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                        LOGGER.warn("RuleExeption: " + stackTrace(ex));
                                    }
                                    if (Rule == null) {
                                        LOGGER.warn("Rule is NUll: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                        continue;
                                    }

                                    if ((!Rule.isIsValidRule()) && (!Rule.isHasNotData())) {
                                        needsave = true;

                                        StartCalendarObj.setTime(CalendarObjRules.getTime());
                                        EndCalendarObj.setTime(CalendarObjRules.getTime());
                                        EndCalendarObj.add(Calendar.HOUR, 1);
                                        EndCalendarObj.set(Calendar.MILLISECOND, 0);
                                        EndCalendarObj.set(Calendar.SECOND, 0);
                                        EndCalendarObj.set(Calendar.MINUTE, 0);
                                        EndCalendarObj.add(Calendar.MILLISECOND, -1);
                                        StartCalendarObj.set(Calendar.MILLISECOND, 0);
                                        StartCalendarObj.set(Calendar.SECOND, 0);
                                        StartCalendarObj.set(Calendar.MINUTE, 0);
                                        ArrayList<Deferred<DataPoints[]>> rule_deferreds = mtrsc.CalculateRulesApachMath(StartCalendarObj.getTimeInMillis(), EndCalendarObj.getTimeInMillis(), m_tsdb);
                                        deferreds.addAll(rule_deferreds);

                                    } 

                                }
                                if (deferreds.size() > 0) {
                                    starttime = System.currentTimeMillis();
                                    Deferred.groupInOrder(deferreds).joinUninterruptibly();
                                    endtime = System.currentTimeMillis() - starttime;
                                    LOGGER.warn("Rule joinUninterruptibly " + CalendarObjRules.getTime() + " to 3 houre time: " + endtime + " Name:" + mtrsc.getName() + " host" + mtrsc.getTags().get("host").getValue());
                                }
                                else {
                                        LOGGER.info("All Rule is Exist: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                        //+ "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue()
                                    }
                                if (needsave) {
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
                                        PutRequest putvalue = new PutRequest(metatable, key, family, qualifiers, values);
                                        m_client.put(putvalue);
                                    } else {
                                        PutRequest putvalue = new PutRequest(metatable, key, family, "n".getBytes(), key);
                                        m_client.put(putvalue);
                                    }
                                }
                                mtrscList.set(mtrsc);
                            }
                        }
                    } catch (JsonSyntaxException ex) {
                        LOGGER.error("JsonSyntaxException: " + stackTrace(ex));
//                this.collector.fail(input);
                    } catch (NumberFormatException ex) {
                        LOGGER.error("NumberFormatException: " + stackTrace(ex));
//                this.collector.fail(input);
                    } catch (Exception ex) {
                        LOGGER.error("Exception: " + stackTrace(ex));
//                        System.exit(0);
//                this.collector.fail(input);
                    }
                    this.jsonResult = null;
                }

            } catch (Exception e) {
                LOGGER.error("Check Error:" + stackTrace(e));

                break;
            }
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
        try {
            m_tsdb.shutdown().joinUninterruptibly();
        } catch (Exception ex) {
            System.out.println("m_tsdb.shutdown: " + ex);

        }
    }

    private String stackTrace(Exception cause) {
        if (cause == null) {
            return "";
        }
        StringWriter sw = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }
}
