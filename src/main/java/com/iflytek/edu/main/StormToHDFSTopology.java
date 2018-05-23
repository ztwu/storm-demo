package com.iflytek.edu.main;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import com.iflytek.edu.Bolt.WordSplitterBolt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/9/14
 * Time: 20:04
 * Description
 */

public class StormToHDFSTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {

        String zks = "192.168.1.100:2181";
        String topic = "ztwu";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "word";
        BrokerHosts brokerHosts = new ZkHosts(zks);

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[] {"192.168.1.100"});
        spoutConf.zkPort = 2181;

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(" : ");
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm/").withPrefix("app_").withExtension(".log");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://192.168.1.101:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter", new WordSplitterBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("hdfs-bolt", hdfsBolt, 2).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = StormToHDFSTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

}
