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
import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.yaml.snakeyaml.Yaml;

public class Main {

    public static void main(String[] args) throws UnsupportedEncodingException {
        try {
            String Filename = null;
            int argindex = 0;
            while (argindex < args.length) {
                System.out.println(args[argindex]);
                if (args[argindex].equals("-c")) {
                    argindex++;
                    Filename = args[argindex];
                }
                argindex++;
            }
            if (Filename == null) {
                Filename = "src/main/resources/config.yaml";
            } else {
                File f = new File(Filename);
                if (!f.exists() || f.isDirectory()) {
                    Filename = "src/main/resources/config.yaml";
                }
            }

//            Config topologyconf = new Config();
            try {
                Yaml yaml = new Yaml();
                java.util.Map rs = (java.util.Map) yaml.load(new InputStreamReader(new FileInputStream(Filename)));
                CalcRulesBoltSheduler Calcer = new CalcRulesBoltSheduler(rs);
                Calcer.prepare();
                Calcer.execute();
//                topologyconf.putAll(rs);
            } catch (FileNotFoundException e) {
                System.out.println(e);
            }
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
