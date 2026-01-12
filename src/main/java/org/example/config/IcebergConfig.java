package org.example.config;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "iceberg")
public class IcebergConfig {

    @Value("${iceberg.catalog.uri}")
    private String catalogUri;

    @Value("${iceberg.catalog.warehouse}")
    private String warehouseLocation;

    @Value("${iceberg.s3.endpoint}")
    private String s3Endpoint;

    @Value("${iceberg.s3.access-key}")
    private String s3AccessKey;

    @Value("${iceberg.s3.secret-key}")
    private String s3SecretKey;

    @Bean
    public Catalog icebergCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", catalogUri);
        properties.put("warehouse", warehouseLocation);
        properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put("s3.endpoint", s3Endpoint);
        properties.put("s3.access-key-id", s3AccessKey);
        properties.put("s3.secret-access-key", s3SecretKey);
        properties.put("s3.path-style-access", "true");
        properties.put("client.region", "us-east-1");

        RESTCatalog restCatalog = new RESTCatalog();
        restCatalog.setConf(new org.apache.hadoop.conf.Configuration());
        restCatalog.initialize("demo", properties);
        return restCatalog;
    }
}
