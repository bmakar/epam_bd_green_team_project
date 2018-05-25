package com.onefoursix;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import static org.apache.hadoop.hdfs.inotify.Event.EventType.CREATE;

public class HdfsINotifyExample {

    public static void main(String[] args) throws IOException, InterruptedException, MissingEventsException {

        long lastReadTxid = 0;

        if (args.length > 1) {
            lastReadTxid = Long.parseLong(args[1]);
        }

        System.out.println("lastReadTxid = " + lastReadTxid);
        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
        HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), conf);

        DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream(lastReadTxid);

        while (true) {
            EventBatch batch = eventStream.take();
            System.out.println("TxId = " + batch.getTxid());
            List<Event> events = Arrays.stream(batch.getEvents()).
                    filter(event -> event.getEventType() == CREATE).collect(Collectors.toList());
            for (Event event : events) {
                CreateEvent createEvent = (CreateEvent) event;
                if (createEvent.getPath().matches("^.*?\\broot-distcp\\b.*?\\bSUCCEEDED\\b.*?$")) {
                    System.out.println(createEvent.getPath());
                    System.out.println(createEvent.getOwnerName());
                }
            }
        }
    }
}

