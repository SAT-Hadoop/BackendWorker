/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import com.amazonaws.services.s3.transfer.TransferManager;
import edu.iit.model.User_Jobs;
import edu.iit.s3bucket.S3Bucket;
import edu.iit.worker.Worker;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
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
    
    
    private static final Logger log = Logger.getLogger( JobExecutionTest.class.getName() );
    public JobExecutionTest() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test @Ignore
    public void runWordCount() {
        Map<String, String> env = System.getenv();
        String home = env.get("HOME");
        System.out.println(home);
        File f = new File("/tmp/inputfile");
        if (!f.exists()){
            System.out.println("No such file buddy");
            System.exit(1);
        }
        Runtime r = Runtime.getRuntime();
        String bin = home + "/hadoop-2.6.0/bin/";
        String sbin = home + "/hadoop-2.6.0/sbin/";
        String jarlocation = home + "/wordcount-1.0-SNAPSHOT.jar";
        String mainclass = "edu.iit.wordcount.WordCount";
        try {
            r.exec(sbin + "stop-all.sh").waitFor();
            r.exec(bin + "hadoop namenode -format -force").waitFor();
            r.exec(sbin + "start-all.sh").waitFor();
            System.out.println("starting up the nodes");
            r.exec(bin + "hadoop fs -rm /input/inputfile").waitFor();
            r.exec(bin + "hadoop fs -rmr /output").waitFor();
            r.exec(bin + "hadoop fs -mkdir /input").waitFor();
            r.exec(bin + "hadoop fs -put /tmp/inputfile /input/").waitFor();
            r.exec(bin + "hadoop jar " + jarlocation +" " +mainclass +" /input/inputfile /output").waitFor();    
            r.exec(bin + "hadoop fs -get /output /tmp/").waitFor();
            S3Bucket s3 = new S3Bucket();
            s3.setBucketname("testresult");
            if (!s3.checkBucket())
                s3.createBucket();
            s3.uploadDirectory("/tmp/output");
            
        } catch (Exception ex) {
            Logger.getLogger(JobExecutionTest.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    @Test
    public void runWorker(){
        Worker worker = new Worker();
        if (worker.checkForMessages()){
            String userjob = worker.getMessages();
            User_Jobs job = worker.getUserJob(userjob);
            String inputurl = job.getInputurl();
            log.warning(job.toString());
            System.out.println(job.toString());
            worker.getInputFile(job.getInputurl());
            
            /*File f = new File("/tmp/output");
            
            if (!f.exists()) {
                System.out.println("sai is sawe");
                System.exit(1);
            }
            S3Bucket s3 = new S3Bucket();
            s3.setBucketname(job.getOutputurl());
            System.out.println("created bucket "+ s3.getBucketName());
            s3.uploadDirectory("/tmp/output");*/
        }
        else{
            System.out.println("no jobs");
        }
        
        //Logger.getLogger(JobExecutionTest.class.getName()).log(Level.SEVERE, null, "message");
    }
    
}
