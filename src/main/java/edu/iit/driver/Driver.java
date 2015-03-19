/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.driver;

import com.amazonaws.services.sqs.model.Message;
import edu.iit.hadoopcluster.HadoopCluster;
import edu.iit.model.User_Jobs;
import edu.iit.worker.Worker;
import java.util.List;

/**
 *
 * @author supramo
 */
public class Driver {
    
    public static void main(String[] args) throws InterruptedException {
        Worker worker = new Worker();
        while (true) {
            if (worker.checkForMessages()) {
                Message message = worker.getMessages();
                String jobid = message.getBody();
                User_Jobs job = worker.getUserJob(jobid);
                List slaves = worker.getSlaves(Integer.parseInt(job.getNodes()));
                if (slaves.size() != Integer.parseInt(job.getNodes())){
                    Thread.sleep(2000);
                    continue;
                }
                worker.addSlavesToCluster(slaves);
                Thread t = new Thread(new HadoopCluster(job));
                t.start();
                
                while (t.isAlive()) {
                    Thread.sleep(1000);
                }
                String filename = worker.renameAndUploadOutput(job);
                worker.deleteMessage(message, job);
                worker.sendmail(job,filename);
                worker.releaseSlaves(slaves);
            } else {
                System.out.println("no Messages we are sleeping");
                Thread.sleep(5000);
            }
        }
        
    }
    
}
