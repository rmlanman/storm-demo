package com.altamiracorp.storm;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.altamiracorp.storm.bolt.SentenceSplit;
import com.altamiracorp.storm.bolt.WordCount;
import com.altamiracorp.storm.spout.TwitterStreamSpout;
import scala.actors.threadpool.Arrays;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class TwitterTermStormRunner {

    private static final String CONFIG_FILE = "storm-demo.properties";

    public static void main (String args []) throws IOException {
        LocalCluster cluster = new LocalCluster();
        Map<Object,Object> conf = getConfig();
        conf.put("terms",  Arrays.asList(args));

        cluster.submitTopology("demo", conf, buildTopology());
    }

    public static StormTopology buildTopology () {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterStreamSpout(),1);
        builder.setBolt("split", new SentenceSplit(), 1).shuffleGrouping("twitter-spout");
        builder.setBolt("word-count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

        return builder.createTopology();
    }

    public static Map<Object, Object> getConfig () throws IOException {
        Properties props = new Properties();
        props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE));
        return props;
    }
}
