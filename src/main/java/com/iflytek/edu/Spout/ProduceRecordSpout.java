package com.iflytek.edu.Spout;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/17
 * Time: 10:52
 *
 * Storm中Spout是一个Topology的消息生产的源头，
 * Spout应该是一个持续不断生产消息的组件，
 * 例如，它可以是一个Socket Server在监听外部Client连接并发送消息，
 * 可以是一个消息队列（MQ）的消费者、可以是用来接收Flume Agent的Sink所发送消息的服务，等等。
 * Spout生产的消息在Storm中被抽象为Tuple，在整个Topology的多个计算组件之间都是根据需要抽象构建的Tuple消息来进行连接，从而形成流。
 *
 * ProduceRecordSpout类是一个Spout组件，用来产生消息，
 * 我们这里模拟发送一些英文句子，实际应用中可以指定任何数据源，
 * 如数据库、消息中间件、Socket连接、RPC调用等等
 *
 * 构造一个ProduceRecordSpout对象时，
 * 传入一个字符串数组，然后随机地选择其中一个句子，
 * emit到下游（Downstream）的WordSplitterBolt组件，
 * 只声明了一个Field，WordSplitterBolt组件可以根据声明的Field，接收到emit的消息
 *
 */

public class ProduceRecordSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(ProduceRecordSpout.class);

    //用来发射数据的工具类
    private SpoutOutputCollector collector;
    private Random random;
    private String[] records;

    public ProduceRecordSpout(String[] records) {
        this.records = records;
    }

    /**
     * 初始化collector
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
    }

    /**
     * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
     */
    public void nextTuple() {
        Utils.sleep(500);
        String record = records[random.nextInt(records.length)];
        List<Object> values = new Values(record);
        // 调用发射方法
        collector.emit(values);
        LOG.info("Record emitted: record=" + record);
    }

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
     * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，该id可以用来定义更加复杂的流拓扑结构
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));//参数要对应
    }
}
