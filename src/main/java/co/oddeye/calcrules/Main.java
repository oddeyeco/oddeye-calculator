/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.calcrules;

/**
 *
 * @author vahan
 */
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) throws UnsupportedEncodingException {
        try {
            CalcRulesBoltSheduler Calcer = new CalcRulesBoltSheduler();
            Calcer.prepare();
            Calcer.execute();
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
