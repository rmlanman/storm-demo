package com.altamiracorp.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.altamiracorp.storm.data.WordCountRepository;

import java.io.IOException;
import java.util.Map;

public class WordCount extends BaseRichBolt {

    private WordCountRepository repository;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        repository = new WordCountRepository((String)stormConf.get("accumuloInstanceName"),
                (String)stormConf.get("zkServers"),
                (String)stormConf.get("accumuloUsername"),
                (String)stormConf.get("accumuloPassword"));
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String word = tuple.getStringByField("word");
            Long wordCount = repository.getWordCount(word);
            if (wordCount == null) {
                wordCount = 0L;
            }
            wordCount++;

            repository.saveWordCount(word,wordCount);
            collector.emit(new Values(word,wordCount));
        } catch (IOException e) {
            collector.fail(tuple);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
