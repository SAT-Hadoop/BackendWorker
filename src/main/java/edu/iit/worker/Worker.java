/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.iit.worker;

import com.amazonaws.services.sqs.model.Message;
import edu.iit.doa.DOA;
import edu.iit.model.User_Jobs;
import edu.iit.sendmail.SendEmail;
import edu.iit.sqs.SendQueue;
import edu.iit.walrus.Walrus;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author supramo
 */
public class Worker{
    SendQueue sendq = new SendQueue();
    DOA doa = new DOA();
    Walrus walrus = new Walrus();
    String queuename;
    public Worker(){
        String ipaddress;
        try {
            ipaddress = Inet4Address.getLocalHost().getHostAddress();
            
            DOA doa = new DOA();
            //this.queuename = "sai3";
            this.queuename = doa.getEc2Queue(ipaddress);
            System.out.println(ipaddress+":"+this.queuename);
            
        } catch (UnknownHostException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
            this.queuename = "sai3";
        }
        
    }
    public boolean checkForMessages(){
        
        //this.queuename = "https://sqs.us-east-1.amazonaws.com/961412573847/sai4";
        return sendq.checkForMessages(this.queuename);
    }
    
    
    public void getInputFile(String filelink) {
        try {
            Runtime r = Runtime.getRuntime();
            walrus.downloadObject("sat-hadoop", filelink);
            r.exec("cp /tmp/"+filelink+" /tmp/inputfile  ").waitFor();
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.WARNING,"Problem downloading the bucket");
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void renameAndUploadOutput(User_Jobs job){
        try {
            Runtime r = Runtime.getRuntime();
            String filename = "/tmp/output" + System.currentTimeMillis();
            r.exec("mv /tmp/output "+filename).waitFor();
            r.exec("/usr/bin/zip "+filename+".zip "+filename).waitFor();
            walrus.putObject("sat-hadoop", filename+".zip");
            job.setOutputurl(filename+".zip");
            job.setJobstatus("COMPLETE");
            doa.updateJob(job);
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.WARNING,"Problem downloading the bucket");
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Message getMessages(){
        return sendq.getMessage();
    }
    
    public User_Jobs getUserJob(String jobid){
        return doa.getUserJob(jobid);
    }
    
    public void deleteMessage(Message message,User_Jobs job){
        sendq.deleteMessage(message, this.queuename);
        job.setJobstatus("COMPLETE");
        doa.updateJob(job);
    }
    
    public void sendmail(User_Jobs job) {
        String to = doa.getUser(job.getUserid()).getEmailid();
        new SendEmail().sendmail("hajek@sat.iit.edu", to);
    }
}

