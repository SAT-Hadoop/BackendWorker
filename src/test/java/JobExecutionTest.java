/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author supramo
 */
public class JobExecutionTest {

    public JobExecutionTest() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test @Ignore
    public void runWordCount() {
        Runtime r = Runtime.getRuntime();
        
        String bin = "/host/DownloadsUbuntu/hadoop-1.2.1/bin/";
        String sbin = "/host/DownloadsUbuntu/hadoop-1.2.1/bin/";
        String jarlocation = "/home/supramo/NetBeansProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar";
        String mainclass = "edu.iit.wordcount.WordCount";
        try {
            r.exec(bin + "stop-all.sh").waitFor();
            r.exec(bin + "hadoop namenode -format -force").waitFor();
            r.exec(bin + "start-all.sh").waitFor();
            r.exec(bin + "hadoop fs -rm /input/inputfile").waitFor();
            r.exec(bin + "hadoop fs -rmr /output").waitFor();
            r.exec(bin + "hadoop fs -mkdir /input").waitFor();
            r.exec(bin + "hadoop fs -put /tmp/inputfile /input/").waitFor();
            r.exec(bin + "hadoop jar " + jarlocation +" " +mainclass +" /input/inputfile /output").waitFor();    
        } catch (Exception ex) {
            Logger.getLogger(JobExecutionTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    @Test
    public void runWorker(){
        
    }
}
