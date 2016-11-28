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
import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import kafka.consumer.KafkaStream;
import net.opentsdb.core.TSDB;
import org.hbase.async.HBaseClient;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.stumbleupon.async.Deferred;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import kafka.consumer.ConsumerIterator;
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
    private OddeeyMetricMetaList mtrscList;
    private boolean needsave;
    private long starttime;
    private long endtime;
    private byte[] key;
    private final byte[] family = "d".getBytes();

    public CalcByKafkaTask(KafkaStream a_stream, int a_threadNumber, TSDB tsdb, HBaseClient client) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        m_client = client;
        m_tsdb = tsdb;
        parser = new JsonParser();
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        byte[] metatable = "test_oddeye-meta".getBytes();
        try {
            mtrscList = new OddeeyMetricMetaList(m_tsdb, metatable);
        } catch (Exception ex) {
            mtrscList = new OddeeyMetricMetaList();
        }        
        Calendar CalendarObjRules = Calendar.getInstance();
        Map<String, MetriccheckRule> Rules ;
        
        while (it.hasNext()) {
            final MessageAndMetadata<byte[], byte[]> message = it.next();
            final String msg = new String(message.message());
//            this.collector.ack(input);
            JsonElement Metric;
            try {
                this.jsonResult = (JsonArray) this.parser.parse(msg);
            } catch (JsonSyntaxException ex) {
                LOGGER.info("msg parse Exception" + ex.toString());
            }
            if (this.jsonResult != null) {
                try {
                    if (this.jsonResult.size() > 0) {
                        LOGGER.debug("Ready count: " + this.jsonResult.size());
                        for (int i = 0; i < this.jsonResult.size(); i++) {
                            Metric = this.jsonResult.get(i);
                            try {
                                final OddeeyMetric metric = new OddeeyMetric(Metric);
                                // Calc Body
                                try {
//                                    OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
//                                    collector.ack(tuple);
                                    OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, m_tsdb);
                                    if (mtrscList.containsKey(mtrsc.hashCode())) {
                                        mtrsc = mtrscList.get(mtrsc.hashCode());
                                    }
                                    CalendarObjRules.setTimeInMillis(metric.getTimestamp());
                                    CalendarObjRules.add(Calendar.HOUR, 1);
                                    CalendarObjRules.add(Calendar.DATE, -1);

                                    Rules = mtrsc.getRules(CalendarObjRules, 7, metatable, m_client);
                                    needsave = false;
                                    final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>();
                                    mtrsc.clearCalcedRulesMap();
                                    for (Map.Entry<String, MetriccheckRule> RuleEntry : Rules.entrySet()) {
                                        final MetriccheckRule l_Rule = RuleEntry.getValue();
                                        Calendar CalObjRules = MetriccheckRule.QualifierToCalendar(l_Rule.getQualifier());
                                        Calendar CalObjRulesEnd = (Calendar) CalObjRules.clone();
                                        CalObjRulesEnd.add(Calendar.HOUR, 1);
                                        CalObjRulesEnd.add(Calendar.MILLISECOND, -1);
                                        if ((!l_Rule.isIsValidRule()) && (!l_Rule.isHasNotData())) {
                                            ArrayList<Deferred<DataPoints[]>> rule_deferreds = mtrsc.CalculateRulesApachMath(CalObjRules.getTimeInMillis(), CalObjRulesEnd.getTimeInMillis(), m_tsdb);
                                            deferreds.addAll(rule_deferreds);
                                        }
                                        if (deferreds.size() > 0) {
                                            needsave = true;
                                            starttime = System.currentTimeMillis();
                                            Deferred.groupInOrder(deferreds).joinUninterruptibly();
                                            endtime = System.currentTimeMillis() - starttime;
                                            LOGGER.info("Rule joinUninterruptibly " + CalendarObjRules.getTime() + " to 1 houre time: " + endtime + " Name:" + mtrsc.getName() + " host" + mtrsc.getTags().get("host").getValue());
                                        } else {
                                            LOGGER.info("All Rule is Exist: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                            //+ "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue()
                                        }
                                        try {

                                            if (needsave) {
                                                key = mtrsc.getKey();
                                                byte[][] qualifiers;
                                                byte[][] values;
                                                ConcurrentMap<String, MetriccheckRule> rulesmap = mtrsc.getCalcedRulesMap();
                                                qualifiers = new byte[rulesmap.size()][];
                                                values = new byte[rulesmap.size()][];
                                                int index = 0;

                                                for (Map.Entry<String, MetriccheckRule> rule : rulesmap.entrySet()) {
                                                    qualifiers[index] = rule.getValue().getQualifier();
                                                    values[index] = rule.getValue().getValues();
                                                    index++;
                                                }
                                                try {

                                                    if (qualifiers.length > 0) {
                                                        PutRequest putvalue = new PutRequest(metatable, key, family, qualifiers, values);
                                                        m_client.put(putvalue);

                                                    } else {
                                                        PutRequest putvalue = new PutRequest(metatable, key, family, "n".getBytes(), key);
                                                        m_client.put(putvalue);
                                                    }
                                                } catch (Exception e) {
                                                    LOGGER.error("catch In Put " + globalFunctions.stackTrace(e));
                                                }

                                            }

                                        } catch (Exception e) {
                                            LOGGER.error("catch In save " + globalFunctions.stackTrace(e));
                                        }

                                        mtrscList.set(mtrsc);
                                    }

                                } catch (Exception ex) {
                                    LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
                                }
                                //end calc body
//                                collector.emit(new Values(mtrsc));
                            } catch (Exception e) {
                                LOGGER.error("Exception: " + globalFunctions.stackTrace(e));
                                LOGGER.error("Exception Wits Metriq: " + Metric);
                                LOGGER.error("Exception Wits Input: " + msg);
                            }

                        }
                    }
                } catch (JsonSyntaxException ex) {
                    LOGGER.error("JsonSyntaxException: " + globalFunctions.stackTrace(ex));
//                this.collector.ack(input);
                } catch (NumberFormatException ex) {
                    LOGGER.error("NumberFormatException: " + globalFunctions.stackTrace(ex));
//                this.collector.ack(input);
                }
                this.jsonResult = null;
            }
        }
    }
}
