package com.iflytek.edu.main;

import com.iflytek.edu.Bolt.WordCounterBolt;
import com.iflytek.edu.Bolt.WordSplitterBolt;
import com.iflytek.edu.Spout.ProduceRecordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/17
 * Time: 11:21
 * Description
 *
 * Storm中Topology的概念类似于Hadoop中的MapReduce Job，
 * 是一个用来编排、容纳一组计算逻辑组件（Spout、Bolt）的对象
 * （Hadoop MapReduce中一个Job包含一组Map Task、Reduce Task），
 * 这一组计算组件可以按照DAG图的方式编排起来（通过选择Stream Groupings来控制数据流分发流向），
 * 从而组合成一个计算逻辑更加负责的对象，那就是Topology。
 * 一个Topology运行以后就不能停止，它会无限地运行下去，除非手动干预（显式执行bin/storm kill ）或意外故障（如停机、整个Storm集群挂掉）让它终止。
 *
 */

public class TopologyTest {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {
        // configure & build topology
        TopologyBuilder builder = new TopologyBuilder();
        String[] records = new String[] {
                "A Storm cluster is superficially similar to a Hadoop cluster",
                "All coordination between Nimbus and the Supervisors is done through a Zookeeper cluster",
                "The core abstraction in Storm is the stream"
        };
        builder
                .setSpout("spout-producer", new ProduceRecordSpout(records), 1)
                .setNumTasks(3);
        builder
                .setBolt("bolt-splitter", new WordSplitterBolt(), 2)
                .shuffleGrouping("spout-producer")
                .setNumTasks(2);
        builder.setBolt("bolt-counter", new WordCounterBolt(), 1)
                .fieldsGrouping("bolt-splitter", new Fields("word"))
                .setNumTasks(2);

        // submit topology
        Config conf = new Config();
        String name = TopologyTest.class.getSimpleName();
        if (args != null && args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }

}
