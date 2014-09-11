package com.altamiracorp.storm.data;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class WordCountRepository {

    private static final String TABLE_NAME = "WordCount";
    private static final String COLUMN_NAME = "word_count";
    private static long MAX_MEMORY = 1000000L;
    private static long MAX_LATENCY = 1000L;
    private static int MAX_WRITE_THREADS = 10;

    private Connector connector;

    public WordCountRepository(String instanceName, String zkServers, String user, String password) {
        try {
            ZooKeeperInstance zooKeeperInstance = new ZooKeeperInstance(instanceName, zkServers);
            connector = zooKeeperInstance.getConnector(user, password.getBytes());
            if (!connector.tableOperations().exists(TABLE_NAME)) {
                connector.tableOperations().create(TABLE_NAME);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Long getWordCount(String word) throws IOException {
        try {
            Scanner scanner = connector.createScanner(TABLE_NAME, new Authorizations());
            scanner.setRange(new Range(word));
            RowIterator rowIterator = new RowIterator(scanner);
            if (rowIterator.hasNext()) {
                Iterator<Map.Entry<Key, Value>> row = rowIterator.next();
                while (row.hasNext()) {
                    Map.Entry<Key, Value> column = row.next();
                    if (column.getKey().getColumnQualifier().toString().equals(COLUMN_NAME)) {
                        return ByteBuffer.wrap(column.getValue().get()).getLong();
                    }
                }

            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return null;
    }

    public void saveWordCount(String word, Long count) throws IOException{
        try {
            BatchWriter writer = connector.createBatchWriter(TABLE_NAME,MAX_MEMORY,MAX_LATENCY,MAX_WRITE_THREADS);
            Mutation mutation = new Mutation(word);
            mutation.put(COLUMN_NAME, COLUMN_NAME, new Value(ByteBuffer.allocate(8).putLong(count).array()));
            writer.addMutation(mutation);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


}
