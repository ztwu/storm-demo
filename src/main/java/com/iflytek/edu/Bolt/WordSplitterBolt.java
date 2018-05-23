package com.iflytek.edu.Bolt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/8/17
 * Time: 11:02
 * Description
 *
 * Storm中消息的处理逻辑被封装到Bolt组件中，
 * 任何处理逻辑都可以在Bolt里面执行，处理过程和普通计算应用程序没什么区别，
 * 只是需要根据Storm的计算语义来合理设置一下组件之间消息流的声明、分发、连接即可。
 * Bolt可以接收来自一个或多个Spout的Tuple消息，也可以来自多个其它Bolt的Tuple消息，也可能是Spout和其它Bolt组合发送的Tuple消息。
 *
 * 在execute方法中，传入的参数是一个Tuple，该Tuple就包含了上游（Upstream）组件ProduceRecordSpout所emit的数据，
 * 直接取出数据进行处理。上面代码中，我们将取出的数据，按照空格进行的split，得到一个一个的单词，
 * 然后在emit到下一个组件，声明的输出schema为2个Field：word和count，当然这里面count的值都为1。
 *
 */

public class WordSplitterBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(WordSplitterBolt.class);
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    //接收喷发节点(Spout)发送的数据进行简单的处理后，发射出去。
    public void execute(Tuple input) {
        String record = input.getString(0);
        if(record != null && StringUtils.isNotEmpty(record.trim())) {
            for(String word : record.split("\\s+")) {
                collector.emit(input, new Values(word, 1));
                LOG.info("Emitted: word=" + word);
                collector.ack(input);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
