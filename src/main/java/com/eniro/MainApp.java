package com.eniro;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.main.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A Camel Application
 */
public class MainApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainApp.class);

    protected static PropertiesComponent props;

    public static void main(String... args) throws Exception {
        Main main = new Main();
        CamelContext ctx = main.getOrCreateCamelContext();
        props = setPropertiesComponent(ctx);
        main.bind("mongoBean", createMongoDBBean(props.getInitialProperties().getProperty("mongo.routers")));
        main.addRouteBuilder(new MyRouteBuilder());

        main.run(args);
    }

    private static PropertiesComponent setPropertiesComponent(CamelContext ctx) throws IOException {
        Properties appProps = new Properties();
        appProps.load(new FileInputStream("/var/local/app.properties"));
        PropertiesComponent prop = new PropertiesComponent();
        prop.setInitialProperties(appProps);
        ctx.addComponent("properties", prop);
        return prop;
    }

    private static MongoClient createMongoDBBean(String mongRouters) {
        LOGGER.info("Mongo routers: {}", mongRouters);
        List<String> routers = Arrays.asList(mongRouters.split(","));
        return new MongoClient(routers.stream()
                .map(router -> {
                    String[] uri = router.split(":");
                    return new ServerAddress(uri[0], Integer.valueOf(uri[1]));
                }).collect(Collectors.toList()));
    }

}

