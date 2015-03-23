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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
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
            Logger.getLogger(Worker.class.getName()).log(Level.WARNING,filelink);
            walrus.downloadObject("sat-hadoop", filelink);
            r.exec("cp /tmp/"+filelink+" /tmp/inputfile  ").waitFor();
        } catch (IOException|InterruptedException ex) {
            Logger.getLogger(Worker.class.getName()).log(Level.WARNING,"Problem downloading the bucket");
            Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public String renameAndUploadOutput(User_Jobs job){
        String filename = "";
        try {
            Runtime r = Runtime.getRuntime();
            filename = "/tmp/output" + System.currentTimeMillis();
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
        return filename+".zip";
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
    
    public void sendmail(User_Jobs job,String filename) {
        String to = doa.getUser(job.getUserid()).getEmailid();
        String message = "Your job is complete, The output is in the file "+filename;
        new SendEmail().sendmail("hajek@sat.iit.edu", to,message);
    }
    
    public List getSlaves(int i){
        return doa.getSlaves(i);
    }
    
    public void releaseSlaves(List slaves){
        for (int i=0;i<slaves.size();i++){
            doa.updateSlave((String)slaves.get(i), "a");
        }
    }
    
    public void addSlavesToCluster(List slaves){
        Map<String, String> env = System.getenv();
        String home = env.get("HOME");
        try{
            Runtime r = Runtime.getRuntime();
            System.out.println("adding master");
            File file = new File(home + "/hadoop-2.6.0/etc/hadoop/masters");
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            output.write(Inet4Address.getLocalHost().getHostAddress());
            output.close();
            file = new File(home + "/hadoop-2.6.0/etc/hadoop/slaves");
            System.out.println(file.getAbsolutePath());
            output = new BufferedWriter(new FileWriter(file));
            output.write(Inet4Address.getLocalHost().getHostAddress());
            
            //r.exec("/bin/echo master > "+ home + "/hadoop-2.6.0/etc/hadoop/slaves").waitFor();
            for (int i=0;i<slaves.size();i++){
                //r.exec("/bin/echo " +(String)slaves.get(i)+ ">> "+ home +"/hadoop-2.6.0/etc/hadoop/slaves").waitFor();
                output.write("\n"+(String)slaves.get(i));
            }
            output.close();
            System.out.println("added slaves");
        }
        catch(Exception e){
            System.out.println(" unable to add");
        }
        
    }
    
}

