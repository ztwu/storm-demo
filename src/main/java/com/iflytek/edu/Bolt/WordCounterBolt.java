package com.iflytek.edu.Bolt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/17
 * Time: 11:10
 * Description
 *
 * Storm中消息的处理逻辑被封装到Bolt组件中，
 * 任何处理逻辑都可以在Bolt里面执行，处理过程和普通计算应用程序没什么区别，
 * 只是需要根据Storm的计算语义来合理设置一下组件之间消息流的声明、分发、连接即可。
 * Bolt可以接收来自一个或多个Spout的Tuple消息，也可以来自多个其它Bolt的Tuple消息，也可能是Spout和其它Bolt组合发送的Tuple消息。
 *
 *
 * 上面代码通过一个Map来对每个单词出现的频率进行累加计数，比较简单。
 * 因为该组件是Topology的最后一个组件，所以不需要在declareOutputFields方法中声明Field的Schema，
 * 而是在cleanup方法中输出最终的结果，只有在该组件结束任务退出时才会调用cleanup方法输出
 *
 *
 */

public class WordCounterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(WordCounterBolt.class);
    private OutputCollector collector;
    private final Map<String, AtomicInteger> counterMap = Maps.newHashMap();

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        int count = input.getIntegerByField("count"); // 通过Field名称取出对应字段的数据
        AtomicInteger ai = counterMap.get(word);
        if(ai == null) {
            ai = new AtomicInteger(0);
            counterMap.put(word, ai);
        }
        ai.addAndGet(count);
        LOG.info("DEBUG: word=" + word + ", count=" + ai.get());
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        // print count results
        LOG.info("Word count results:");
        for(Map.Entry<String, AtomicInteger> entry : counterMap.entrySet()) {
            LOG.info("\tword=" + entry.getKey() + ", count=" + entry.getValue().get());
        }
    }

}
