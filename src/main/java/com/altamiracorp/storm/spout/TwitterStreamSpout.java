package com.altamiracorp.storm.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamSpout extends BaseRichSpout {

    private Client hbc;
    private SpoutOutputCollector collector;
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<String> terms = (List<String>) conf.get("terms");
        endpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1((String)conf.get("consumerKey"),
                (String)conf.get("consumerSecret"),
                (String)conf.get("token"),
                (String)conf.get("tokenSecret"));

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-spout")
                .hosts(hosebirdHosts)
                .endpoint(endpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(msgQueue));

        hbc = builder.build();
        hbc.connect();
    }

    @Override
    public void close() {
        hbc.stop();
    }

    @Override
    public void nextTuple() {
        try {
            collector.emit(new Values(msgQueue.take()));
        } catch (InterruptedException e) {
            collector.reportError(e);
        }
    }

}
