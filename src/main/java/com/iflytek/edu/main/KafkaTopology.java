package com.iflytek.edu.main;

import com.iflytek.edu.Bolt.WordSplitterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/18
 * Time: 11:31
 * Description
 */

public class KafkaTopology {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AlreadyAliveException, AuthorizationException {
        String zks = "192.168.1.100:2181";
        String topic = "ztwu";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "word";
        BrokerHosts brokerHosts = new ZkHosts(zks);

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[] {"192.168.1.100"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter", new WordSplitterBolt(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new TestWordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = KafkaTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            // Nimbus host name passed from command line
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
