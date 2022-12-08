package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.Instant;


public class Sender {

    private static String queue_name = "SdAssignment2";
    private static String host_name = "goose.rmq2.cloudamqp.com";
    private static String username = "eesuskpb";
    private static String virtual_host_name = "eesuskpb";
    private static String password = "4BySMiy_hHc7P9gZMxB14XdOBfZqzbel";


    private static ConnectionFactory connection(){
        ConnectionFactory conn = new ConnectionFactory();
        conn.setHost(host_name);
        conn.setUsername(username);
        conn.setVirtualHost(virtual_host_name);
        conn.setPassword(password);
        conn.setRequestedHeartbeat(30);
        conn.setConnectionTimeout(30000);
        return conn;
    }

    public static void main(String[] argv) throws Exception {

        ConnectionFactory connectionFactory = connection();

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(queue_name,true,false,false,null);

        FileReader file = new FileReader("sensor.csv");
        BufferedReader csvFile = new BufferedReader(file);

        String newMeasurement ="";
        String newLine = null;

        while((newLine = csvFile.readLine()) != null){
            newMeasurement = newLine.split(",")[0];
             String meas = Instant.now()+","+newMeasurement;

            channel.basicPublish("",queue_name,null,meas.getBytes());
            System.out.println(" X Sent : "+meas+" ;");
            Thread.sleep(1000);

        }



    }
}
