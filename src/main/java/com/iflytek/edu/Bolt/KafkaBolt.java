package com.iflytek.edu.Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with Intellij IDEA.
 * User: ztwu2
 * Date: 2017/9/20
 * Time: 14:54
 * Description
 */

public class KafkaBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
    }

    public void execute(Tuple input) {
        String temp = input.getString(0);
        System.out.println(temp);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
