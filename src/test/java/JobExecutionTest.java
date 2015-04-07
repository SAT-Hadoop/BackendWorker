/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import com.amazonaws.services.sqs.model.Message;
import edu.iit.driver.Driver;
import edu.iit.model.User_Jobs;
import edu.iit.worker.Worker;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.Ignore;

/**
 *
 * @author supramo
 */
public class JobExecutionTest {

    private static final Logger log = Logger.getLogger(JobExecutionTest.class.getName());

    public JobExecutionTest() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    @Test
    public void runWordCount() throws InterruptedException {
        Map<String, String> env = System.getenv();
        String home = env.get("HOME");
        System.out.println(home);
        Worker worker = new Worker();

        User_Jobs job = new User_Jobs();

        Message message = new Message();
        while (true) {

            if (worker.checkForMessages()) {
                message = worker.getMessages();
                System.out.println(message.getBody());
                job = worker.getUserJob(worker.getMessages().getBody());
                worker.getInputFile(job.getInputurl());
                System.out.println(job.toString());
                worker.getInputFile(job.getInputurl());
                System.exit(1);
                
                List slaves = worker.getSlaves(Integer.parseInt(job.getNodes()));
                while (slaves.size() != Integer.parseInt(job.getNodes())){
                    slaves = worker.getSlaves(Integer.parseInt(job.getNodes()));
                    System.out.println("Slaves not free, waiting");
                    Thread.sleep(5000);
                }
                worker.addSlavesToCluster(slaves);
                if (!worker.makeSlaves(slaves)) {
                    worker.releaseSlaves(slaves);
                    System.exit(1);
                }
                File f = new File("/tmp/inputfile");
                if (!f.exists()) {
                    System.out.println("No such file buddy");
                    System.exit(1);
                }

                
                Runtime r = Runtime.getRuntime();
                String bin = home + "/hadoop-2.6.0/bin/";
                String sbin = home + "/hadoop-2.6.0/sbin/";
                String jarlocation = home + "/wordcount-1.0-SNAPSHOT.jar";
                String mainclass = "edu.iit.wordcount.WordCount";
                String jobtype = job.getJobname();
                if (jobtype.equals("wordcount")){
                    jarlocation = home + "/wordcount-1.0-SNAPSHOT.jar";
                }
                else {
                    jarlocation = home + "/MarketBasket-1.0-SNAPSHOT.jar";
                }
                if (!new File(jarlocation).exists()) {
                    System.out.println("No such jar buddy, please kick the admins butt");
                    System.exit(1);
                }
                try {
                    r.exec(sbin + "stop-all.sh").waitFor();
                    FileUtils.cleanDirectory(new File("/tmp/hadoop-root/dfs/data/"));
                    //r.exec("/bin/rm -rf /tmp/hadoop-root/dfs/data/*").waitFor();
                    r.exec(bin + "hadoop namenode -format -force").waitFor();
                    r.exec(sbin + "start-all.sh").waitFor();
                    System.out.println("starting up the nodes");
                    r.exec(bin + "hadoop fs -rm /input/inputfile").waitFor();
                    r.exec(bin + "hadoop fs -rmr /output").waitFor();
                    r.exec(bin + "hadoop fs -mkdir /input").waitFor();
                    r.exec(bin + "hadoop fs -put /tmp/inputfile /input/").waitFor();
                    r.exec(bin + "hadoop jar " + jarlocation + " " + mainclass + " /input/inputfile /output").waitFor();
                    r.exec(bin + "hadoop fs -get /output /tmp/").waitFor();
                    r.exec(sbin + "stop-all.sh").waitFor();
                } catch (Exception ex) {
                    Logger.getLogger(Driver.class.getName()).log(Level.SEVERE, null, ex);
                }
                String filename = worker.renameAndUploadOutput(job);
                worker.deleteMessage(message, job);
                worker.sendmail(job, filename);
                worker.releaseSlaves(slaves);
            } else {
                Thread.sleep(5000);
            }

        }

    }

    @Test
    @Ignore
    public void getInputFile() {
        Worker worker = new Worker();
        if (worker.checkForMessages()) {
            User_Jobs job = worker.getUserJob(worker.getMessages().getBody());
            System.out.println(job.getJobid());
            System.out.println(job.toString());
            worker.getInputFile(job.getInputurl());
            //worker.renameAndUploadOutput(job);
        }

    }

    @Test
    @Ignore
    public void runWorker() {
        /*String filename = "wordcount.input";
         Runtime r = Runtime.getRuntime();
         Walrus walrus = new Walrus();
         walrus.putObject("sat-hadoop", "/tmp/output");*/

        /*try {
         r.exec("mv "+"/tmp/" + filename+" /tmp/inputfile").waitFor();
         } catch (Exception ex) {
         Logger.getLogger(JobExecutionTest.class.getName()).log(Level.SEVERE, null, ex);
         } */
    }

}
