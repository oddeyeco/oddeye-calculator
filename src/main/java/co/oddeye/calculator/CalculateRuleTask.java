/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calculator;

import static co.oddeye.calculator.MainClass.client;
import static co.oddeye.calculator.MainClass.tsdb;
import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetricMeta;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CalculateRuleTask implements Runnable {

    private final int m_threadNumber;
    private static final Logger LOGGER = LoggerFactory.getLogger(CalculateRuleTask.class);
    private final OddeeyMetricMeta mtrsc;
    private final Calendar EndCalendarObj;
    private final Calendar StartCalendarObj;
    private byte[] key;
    private final String metatable;
    private static final byte[] family = "d".getBytes();

    /**
     *
     * @param a_threadNumber
     * @param m
     * @param start
     * @param end
     */
    public CalculateRuleTask(int a_threadNumber, OddeeyMetricMeta m, Calendar start, Calendar end, String table) {
        m_threadNumber = a_threadNumber;
        mtrsc = m;
        EndCalendarObj = end;
        StartCalendarObj = start;
        metatable = table;
                
        
    }

    ;    
    
    @Override
    public void run() {

        try {
            //                if (!mtrsc.getName().equals("cpu_guest_nice"))
//                {
//                    continue;
//                }
//                if (!mtrsc.getTags().get("host").getValue().equals("esd10.mouseflow.us"))                
//                {
//                    continue;
//                }
            LOGGER.warn(mtrsc.getName() + " " + mtrsc.getTags().toString());
            long starttime = System.currentTimeMillis();
//                EndCalendarObj.set(Calendar.HOUR, 10);
//                EndCalendarObj.set(Calendar.DATE, 24);
//                MetriccheckRule Rule = mtrsc.getRule(StartCalendarObj, metatable.getBytes(), client);
//                GetRequest get = new GetRequest(metatable.getBytes(), mtrsc.getKey());
//                final ArrayList<KeyValue> ruledata = client.get(get).joinUninterruptibly();
//                for (final KeyValue kv : ruledata) {
//                    final byte[] timekey = kv.qualifier();
//                    if (Arrays.equals(timekey, "n".getBytes()))
//                    {
//                        continue;
//                    }
//                    CalendarObj.setTimeInMillis(0);
//                    byte[] b_value = Arrays.copyOfRange(timekey, 0, 2);
//                    CalendarObj.set(Calendar.YEAR, ByteBuffer.wrap(b_value).getShort());
//                    b_value = Arrays.copyOfRange(timekey, 2, 4);
//                    CalendarObj.set(Calendar.DAY_OF_YEAR, ByteBuffer.wrap(b_value).getShort());
//                    b_value = Arrays.copyOfRange(timekey, 4, 6);
//                    CalendarObj.set(Calendar.HOUR_OF_DAY, ByteBuffer.wrap(b_value).getShort());
//                    System.out.println(CalendarObj.getTime());
//
//                }
//                System.out.println(Rule);                
//                LOGGER.warn(mtrsc.getName() + " " + mtrsc.getTags().toString());
//                continue;

//                for (int j = 0; j < daycount; j++) {
            mtrsc.CalculateRulesAsync(StartCalendarObj.getTimeInMillis(), EndCalendarObj.getTimeInMillis(), tsdb);
//                    StartCalendarObj.add(Calendar.DATE, -1);
//                    EndCalendarObj.add(Calendar.DATE, -1);
//                }

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
            long endtime = System.currentTimeMillis() - starttime;
//                if (endtime>300)
//                {
            LOGGER.warn(m_threadNumber + " done in " + endtime + " ms");
//                }            
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(CalculateRuleTask.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
