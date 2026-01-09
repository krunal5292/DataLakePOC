package org.example.processing.strategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import jakarta.annotation.PostConstruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.example.consent.model.ComplexConsentRule;
import org.example.ingestion.model.TelemetryMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Advanced Iceberg-based Physical Partitioning Strategy.
 * Implements the "Fan-Out" pattern using Apache Iceberg tables.
 */
@Component
@ConditionalOnProperty(name = "consent.enforcement.strategy", havingValue = "iceberg")
public class IcebergPhysicalPartitionStrategy implements GoldEnforcementStrategy {

    private final MinioClient minioClient;
    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redisTemplate;

    @Value("${minio.bucket.silver}")
    private String silverBucket;

    // Iceberg Config
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

    private Catalog catalog;
    private TableIdentifier tableIdentifier;
    private static final String NAMESPACE = "gold";

    @Value("${iceberg.table.name:telemetry}")
    private String tableName;

    // Schema Definition
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "trace_id", Types.StringType.get()),
            Types.NestedField.required(2, "athlete_id", Types.StringType.get()),
            Types.NestedField.required(3, "timestamp", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "purpose", Types.StringType.get()),
            Types.NestedField.optional(5, "activity_type", Types.StringType.get()),
            Types.NestedField.optional(6, "event_id", Types.StringType.get()),
            Types.NestedField.required(7, "data", Types.StringType.get()) // JSON Payload
    );

    @Value("${iceberg.partition.spec:purpose,activity_type,event_id,day:timestamp}")
    private String partitionSpecString;

    public IcebergPhysicalPartitionStrategy(MinioClient minioClient, ObjectMapper objectMapper,
            StringRedisTemplate redisTemplate) {
        this.minioClient = minioClient;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void initialize() {
        System.out.println("Initializing Iceberg Strategy...");

        // 0. Ensure Warehouse Bucket Exists in MinIO
        try

        {
            String bucketName = "gold-warehouse"; // Assuming simple s3://bucket/ structure or from property
            if (warehouseLocation.startsWith("s3://")) {
                bucketName = warehouseLocation.substring(5).split("/")[0];
            }
            boolean found = minioClient.bucketExists(io.minio.BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(io.minio.MakeBucketArgs.builder().bucket(bucketName).build());
                System.out.println("Created warehouse bucket: " + bucketName);
            }
        } catch (Exception e) {
            System.err.println("Failed to validation/create warehouse bucket: " + e.getMessage());
            // Continue, maybe it exists or catalog handles it differently (though unlikely
            // for S3FileIO)
        }

        // 1. Configure Catalog
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
        restCatalog.setConf(new Configuration());
        restCatalog.initialize("demo", properties);
        this.catalog = restCatalog;

        // 2. Create Table if not exists
        this.tableIdentifier = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);

        // Ensure namespace exists
        try {
            if (catalog instanceof RESTCatalog) {
                // REST Catalog supports namespaces usually but checking might fail
                if (!((RESTCatalog) catalog).namespaceExists(Namespace.of(NAMESPACE))) {
                    ((RESTCatalog) catalog).createNamespace(Namespace.of(NAMESPACE));
                }
            }
        } catch (Exception e) {
            System.out.println("Namespace creation skipped/failed: " + e.getMessage());
        }

        if (!catalog.tableExists(tableIdentifier)) {
            System.out.println("Creating Iceberg table: " + tableIdentifier);
            System.out.println("Using Partition Spec: " + partitionSpecString);

            // Dynamic Partition Spec Builder
            org.apache.iceberg.PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(SCHEMA);
            String[] fields = partitionSpecString.split(",");
            for (String field : fields) {
                String[] parts = field.trim().split(":");
                if (parts.length == 2) {
                    // Transform, e.g., day:timestamp
                    String transform = parts[0];
                    String source = parts[1];
                    switch (transform.toLowerCase()) {
                        case "day":
                            specBuilder.day(source);
                            break;
                        case "hour":
                            specBuilder.hour(source);
                            break;
                        case "month":
                            specBuilder.month(source);
                            break;
                        case "year":
                            specBuilder.year(source);
                            break;
                        case "bucket":
                            // Simplified bucket support (bucket[16]:id), assuming default N for now or
                            // parse further
                            specBuilder.bucket(source, 16);
                            break;
                        default:
                            System.err.println("Unknown transform: " + transform + ", defaulting to identity");
                            specBuilder.identity(source);
                    }
                } else {
                    // Identity
                    specBuilder.identity(field.trim());
                }
            }
            PartitionSpec spec = specBuilder.build();

            catalog.createTable(tableIdentifier, SCHEMA, spec);
        } else {
            System.out.println("Iceberg table already exists: " + tableIdentifier);
        }
    }

    @Override
    public void processAndFanOut(TelemetryMessage message) {
        try {
            // 1. Read Enriched Silver Data
            System.out.println("Reading MinIO: " + message.getMinioPath());
            Map<String, Object> enrichedData;
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(silverBucket).object(message.getMinioPath()).build())) {
                enrichedData = objectMapper.readValue(stream, Map.class);
            }
            System.out.println("Enriched Data: " + enrichedData);

            // 2. Fetch Consent Rule
            String ruleKey = "consent:rule:" + message.getAthleteId();
            String ruleJson = redisTemplate.opsForValue().get(ruleKey);
            System.out.println("Redis Key: " + ruleKey + ", Value: " + ruleJson);

            if (ruleJson == null) {
                System.out.println("Rule not found in Redis.");
                return;
            }
            ComplexConsentRule rule = objectMapper.readValue(ruleJson, ComplexConsentRule.class);

            if (!"ACTIVE".equals(rule.getStatus())) {
                System.out.println("Rule not ACTIVE: " + rule.getStatus());
                return;
            }

            // 3. Extract Metadata
            String activity = (String) enrichedData.getOrDefault("activity_type", "unknown");
            String eventId = (String) enrichedData.getOrDefault("event_id", "unknown");

            // 4. Fan-Out per Purpose
            ComplexConsentRule.DimensionDetail purposeDim = rule.getDimensions().getPurpose();
            if (purposeDim != null && purposeDim.getValues() != null) {
                System.out.println("Fanning out for purposes: " + purposeDim.getValues().size());
                Table table = catalog.loadTable(tableIdentifier);

                for (ComplexConsentRule.ValueDetail purpose : purposeDim.getValues()) {
                    System.out.println("Writing record for purpose: " + purpose.getValue());
                    writeRecord(table, message, enrichedData, purpose.getValue(), activity, eventId);
                }
            } else {
                System.out.println("No purpose dimension found.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Iceberg Strategy Failed", e);
        }
    }

    private void writeRecord(Table table, TelemetryMessage message, Map<String, Object> data,
            String purpose, String activity, String eventId) throws Exception {

        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("trace_id", message.getTraceId());
        record.setField("athlete_id", message.getAthleteId());
        record.setField("timestamp", Instant.ofEpochMilli(message.getTimestamp()).atOffset(ZoneOffset.UTC));
        record.setField("purpose", purpose);
        record.setField("activity_type", activity);
        record.setField("event_id", eventId);
        record.setField("data", objectMapper.writeValueAsString(data));

        // Create a unique file path for this record
        String filename = String.format("data/%s/%s/%s.parquet", purpose, activity, UUID.randomUUID());
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + filename);

        org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(SCHEMA)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .setAll(table.properties())
                .build();

        appender.add(record);
        appender.close();

        // Create Partition Key from Record
        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(table.spec(),
                table.schema());

        // Manual population to avoid type issues with OffsetDateTime vs Long (internal)
        partitionKey.set(0, purpose); // identity(purpose)
        partitionKey.set(1, activity); // identity(activity_type)
        partitionKey.set(2, eventId); // identity(event_id)

        // Calculate days from epoch for timestamp (day transform)
        int days = (int) (message.getTimestamp() / (1000 * 60 * 60 * 24));
        partitionKey.set(3, days);

        DataFile dataFile = DataFiles.builder(table.spec())
                .withPath(outputFile.location())
                .withFileSizeInBytes(appender.length())
                .withPartition(partitionKey)
                .withRecordCount(appender.metrics().recordCount())
                .withFormat(FileFormat.PARQUET)
                .build();

        table.newAppend().appendFile(dataFile).commit();
        System.out.println("Committed Iceberg file: " + outputFile.location());
    }

    private org.apache.iceberg.StructLike extractPartition(Record record, Table table) {
        // Not used in the manual path construction above
        return null;
    }

    @Override
    public void handleRevocation(String athleteId, String purpose) {
        System.out.println("Handling revocation for: " + athleteId + ", Purpose: " + purpose);
        Table table = catalog.loadTable(tableIdentifier);

        // Find files that contain relevant data
        // Note: This scans files based on partition pruning (purpose), then reads them
        // to check functionality.
        // In a real high-volume scenario, we might use an index or stricter
        // partitioning.
        CloseableIterable<FileScanTask> tasks = table.newScan()
                .filter(Expressions.equal("purpose", purpose))
                .planFiles();

        for (FileScanTask task : tasks) {
            DataFile file = task.file();
            System.out.println("Processing file for revocation: " + file.path());

            // Read records
            java.util.List<Record> allRecords = new java.util.ArrayList<>();
            try (CloseableIterable<Object> reader = Parquet.read(table.io().newInputFile(file.path().toString()))
                    .project(table.schema())
                    .build()) {
                for (Object obj : reader) {
                    if (obj instanceof org.apache.avro.generic.GenericRecord) {
                        org.apache.avro.generic.GenericRecord avroRec = (org.apache.avro.generic.GenericRecord) obj;
                        GenericRecord icebergRec = GenericRecord.create(SCHEMA);
                        icebergRec.setField("trace_id", String.valueOf(avroRec.get("trace_id")));
                        icebergRec.setField("athlete_id", String.valueOf(avroRec.get("athlete_id")));

                        Object ts = avroRec.get("timestamp");
                        if (ts instanceof Long) {
                            long micros = (Long) ts;
                            // Iceberg stores micros, converting to OffsetDateTime for Record
                            icebergRec.setField("timestamp",
                                    Instant.ofEpochMilli(micros / 1000).atOffset(ZoneOffset.UTC));
                        } else {
                            icebergRec.setField("timestamp", ts);
                        }

                        icebergRec.setField("purpose", String.valueOf(avroRec.get("purpose")));
                        icebergRec.setField("activity_type",
                                avroRec.get("activity_type") != null ? String.valueOf(avroRec.get("activity_type"))
                                        : null);
                        icebergRec.setField("event_id",
                                avroRec.get("event_id") != null ? String.valueOf(avroRec.get("event_id")) : null);
                        icebergRec.setField("data", String.valueOf(avroRec.get("data")));

                        allRecords.add(icebergRec);
                    } else if (obj instanceof Record) {
                        allRecords.add((Record) obj);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read file: " + file.path(), e);
            }

            java.util.List<Record> keptRecords = new java.util.ArrayList<>();
            for (Record r : allRecords) {
                // Check if this record belongs to the revoked user AND purpose
                String rAthlete = (String) r.getField("athlete_id");
                String rPurpose = (String) r.getField("purpose");

                if (athleteId.equals(rAthlete) && purpose.equals(rPurpose)) {
                    // Drop it
                    continue;
                }
                keptRecords.add(r);
            }

            if (keptRecords.size() != allRecords.size()) {
                if (keptRecords.isEmpty()) {
                    System.out.println("Deleting file as it contains only revoked data: " + file.path());
                    table.newDelete().deleteFile(file).commit();
                } else {
                    // Write new file
                    System.out.println("Rewriting file with " + keptRecords.size() + " records: " + file.path());
                    try {
                        Record sample = keptRecords.get(0);
                        String rPurpose = (String) sample.getField("purpose");
                        String rActivity = (String) sample.getField("activity_type");
                        // String rEventId = (String) sample.getField("event_id");

                        String newFilename = String.format("data/%s/%s/%s.parquet", rPurpose, rActivity,
                                UUID.randomUUID());
                        OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + newFilename);

                        org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                                .schema(SCHEMA)
                                .createWriterFunc(GenericParquetWriter::buildWriter)
                                .overwrite()
                                .setAll(table.properties())
                                .build();

                        for (Record r : keptRecords) {
                            appender.add(r);
                        }
                        appender.close();

                        // Partition Data
                        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(table.spec(),
                                table.schema());

                        // Partition logic matches writeRecord
                        partitionKey.set(0, sample.getField("purpose"));
                        partitionKey.set(1, sample.getField("activity_type"));
                        partitionKey.set(2, sample.getField("event_id"));

                        Object tsObj = sample.getField("timestamp");
                        int days = 0;
                        if (tsObj instanceof java.time.OffsetDateTime) {
                            days = (int) ((java.time.OffsetDateTime) tsObj).toLocalDate().toEpochDay();
                        } else if (tsObj instanceof Long) {
                            // Internal representation (micros)
                            days = (int) ((Long) tsObj / (1000L * 1000 * 60 * 60 * 24));
                        } else {
                            // Fallback or error
                            days = (int) (Instant.now().toEpochMilli() / (1000 * 60 * 60 * 24));
                        }
                        partitionKey.set(3, days);

                        DataFile newFile = DataFiles.builder(table.spec())
                                .withPath(outputFile.location())
                                .withFileSizeInBytes(appender.length())
                                .withPartition(partitionKey)
                                .withRecordCount(appender.metrics().recordCount())
                                .withFormat(FileFormat.PARQUET)
                                .build();

                        table.newRewrite().rewriteFiles(java.util.Set.of(file), java.util.Set.of(newFile)).commit();

                    } catch (Exception e) {
                        throw new RuntimeException("Failed to rewrite file", e);
                    }
                }
            }
        }
    }

    @Override
    public void handleGrant(String athleteId, String purpose) {
        System.out.println("Grant not yet implemented for Iceberg strategy.");
    }

    @Override
    public boolean verifyDataExists(String athleteId, String purpose) {
        Table table = catalog.loadTable(tableIdentifier);

        CloseableIterable<Record> results = IcebergGenerics.read(table)
                .where(Expressions.and(
                        Expressions.equal("athlete_id", athleteId),
                        Expressions.equal("purpose", purpose)))
                .build();

        try {
            return results.iterator().hasNext();
        } catch (Exception e) {
            return false;
        }

    }

    @Override
    public String getStrategyName() {
        return "iceberg";
    }

    /**
     * Manual Re-indexing / Compaction.
     * Reads ALL data from the table, groups it by the CURRENT partition spec, and
     * rewrites it.
     * WARNING: In-memory operation. suitable for small datasets only.
     */
    public void reindexData(String tableName) {
        TableIdentifier id = TableIdentifier.of("gold", tableName);
        if (!catalog.tableExists(id)) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        Table table = catalog.loadTable(id);

        System.out.println("Starting re-indexing for table: " + tableName);
        System.out.println("Current Spec: " + table.spec());

        // 1. Identify Files to Delete (ALL Existing Files)
        Set<DataFile> filesToDelete = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                filesToDelete.add(task.file());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan files", e);
        }

        if (filesToDelete.isEmpty()) {
            System.out.println("No data to re-index.");
            return;
        }

        // 2. Read All Records
        List<Record> allRecords = new ArrayList<>();
        try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
            for (Record record : reader) {
                // We must clone or copy the record because the reader might reuse objects
                // and we need to process them. Ideally, we stream this.
                // For this POC, collecting to list is acceptable but NOT for production.
                // Note: Iceberg Generics reader returns GenericRecord which is usually safe to
                // keep if not detached?
                // Actually, let's create a new GenericRecord to be safe and ensure schema
                // compatibility if evolved.
                GenericRecord newRec = GenericRecord.create(table.schema());
                for (org.apache.iceberg.types.Types.NestedField field : table.schema().columns()) {
                    newRec.setField(field.name(), record.getField(field.name()));
                }
                allRecords.add(newRec);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read records", e);
        }

        // 3. Group by Partition Key (New Spec)
        Map<PartitionKey, List<Record>> partitionedData = new HashMap<>();
        for (Record record : allRecords) {
            PartitionKey key = new PartitionKey(table.spec(), table.schema());
            key.partition(record);
            partitionedData.computeIfAbsent(key.copy(), k -> new ArrayList<>()).add(record);
        }

        // 4. Write New Files
        Set<DataFile> newFiles = new HashSet<>();
        for (Map.Entry<PartitionKey, List<Record>> entry : partitionedData.entrySet()) {
            PartitionKey key = entry.getKey();
            List<Record> records = entry.getValue();

            if (records.isEmpty())
                continue;

            String filename = "data/" + tableName + "/reindexed-" + UUID.randomUUID() + ".parquet";
            OutputFile outputFile = table.io().newOutputFile(table.location() + "/" + filename);

            try (org.apache.iceberg.io.FileAppender<Record> appender = Parquet.write(outputFile)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::buildWriter)
                    .overwrite()
                    .build()) {

                for (Record r : records) {
                    appender.add(r);
                }
                appender.close();

                DataFile dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
                        .withPath(outputFile.location())
                        .withFileSizeInBytes(appender.length())
                        .withPartition(key)
                        .withRecordCount(records.size())
                        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
                        .build();
                newFiles.add(dataFile);

            } catch (IOException e) {
                throw new RuntimeException("Failed to write re-indexed file", e);
            }
        }

        // 5. Commit Rewrite
        table.newRewrite()
                .rewriteFiles(filesToDelete, newFiles)
                .commit();

        System.out.println("Re-indexing complete. Replaced " + filesToDelete.size() + " files with " + newFiles.size()
                + " files.");
    }
}
