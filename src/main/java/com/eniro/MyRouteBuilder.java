package com.eniro;

import com.mongodb.BasicDBObject;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class MyRouteBuilder extends RouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyRouteBuilder.class);

    private AtomicInteger processedMessages = new AtomicInteger(0);

    public void configure() {
        Properties props = MainApp.props.getInitialProperties();
        String kafkaUri =
                String.format("kafka:%s?brokers=%s&consumersCount=%s&groupId=%s&seekTo=%s",
                        props.getProperty("kafka.topic"), props.getProperty("kafka.brokers"),
                        props.getProperty("kafka.consumersCount"), props.getProperty("kafka.groupId"),
                        props.getProperty("kafka.seekTo"));

        String mongoUri =
                String.format("mongodb:mongoBean?database=%s&collection=%s&operation=insert",
                        props.getProperty("mongo.db"),
                        props.getProperty("mongo.collection"));

        LOGGER.info("Kafka Uri: {}", kafkaUri);
        LOGGER.info("Mongo Uri: {}", mongoUri);

        from(kafkaUri)
                .to("log:com.eniro.kafka2mongo?level=DEBUG&showHeaders=true")
                .process(
                        exchange -> {
                            String messageKey = "";
                            if (exchange.getIn() != null) {
                                Message message = exchange.getIn();
                                Integer partitionId = (Integer) message
                                        .getHeader(KafkaConstants.PARTITION);
                                Long offset = (Long) message.getHeader(KafkaConstants.OFFSET);
                                if (message.getHeader(KafkaConstants.KEY) != null)
                                    messageKey = (String) message
                                            .getHeader(KafkaConstants.KEY);

                                BasicDBObject dbObj = BasicDBObject.parse((String) message.getBody());
                                dbObj.append("key", messageKey);
                                dbObj.append("partition", partitionId);
                                dbObj.append("offset", offset);

                                int flag = processedMessages.incrementAndGet();
                                if (flag % 10000 == 0)
                                    LOGGER.info("Progress: {} messages processed by thread {}, current object: {}", flag, Thread.currentThread().getId(), dbObj.toJson());

                                exchange.getOut().setBody(dbObj);
                            }
                        })
                .to(mongoUri);
    }

}
