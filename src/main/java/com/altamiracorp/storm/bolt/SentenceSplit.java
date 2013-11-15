package com.altamiracorp.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.JSONObject;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentenceSplit extends BaseRichBolt {

    private OutputCollector collector;
    private Pattern wordPattern;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.wordPattern =  Pattern.compile("[\\w']*");
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("tweet");
        JSONObject tweetJson = new JSONObject(tweet);

        //if an actual tweet, process it
        if (tweetJson.has("text")) {

            String [] words = tweetJson.getString("text").split(" ");

            Matcher m;
            for (String word : words) {
                m = wordPattern.matcher(word);
                if (m.find()) {
                    collector.emit(new Values(m.group()));
                }
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
