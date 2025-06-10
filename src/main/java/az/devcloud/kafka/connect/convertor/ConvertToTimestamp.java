package az.devcloud.kafka.connect.convertor;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.util.Date;
import java.util.Map;

public class ConvertToTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ConvertToTimestamp.class);
    private static final ZoneId DEFAULT_ZONE = ZoneId.of("Asia/Baku");

    // Updated config definition with field.name
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Name of the field containing the timestamp to convert")
            .define("timezone", ConfigDef.Type.STRING, DEFAULT_ZONE.getId(),
                    ConfigDef.Importance.MEDIUM, "Target timezone ID");

    private String fieldName;
    private ZoneId targetZone;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldName = config.getString("field.name");
        this.targetZone = ZoneId.of(config.getString("timezone"));
        log.info("Configured to convert field '{}' to timezone {}", fieldName, targetZone);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null || !(record.value() instanceof Struct)) {
            log.warn("Skipping record with null or non-Struct value");
            return record;
        }

        Struct payload = (Struct) record.value();
        Struct after = (Struct) payload.get("after");
        if (after == null) {
            log.warn("Skipping record with null 'after' field");
            return record;
        }

        // Validate field exists
        if (after.schema().field(fieldName) == null) {
            throw new DataException("Field '" + fieldName + "' not found in record");
        }

        // Build new schema with timestamp type
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Field field : after.schema().fields()) {
            schemaBuilder.field(field.name(),
                    field.name().equals(fieldName) ? Timestamp.SCHEMA : field.schema());
        }

        // Create new struct with converted timestamp
        Struct newStruct = new Struct(schemaBuilder.build());
        for (Field field : after.schema().fields()) {
            if (field.name().equals(fieldName)) {
                try {
                    Date converted = convertToTargetTime(after.getString(fieldName));
                    newStruct.put(field.name(), converted);
                    log.debug("Converted {} from {} to {}", fieldName,
                            after.getString(fieldName), converted);
                } catch (Exception e) {
                    throw new DataException("Failed to convert timestamp", e);
                }
            } else {
                newStruct.put(field.name(), after.get(field));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newStruct.schema(),
                newStruct,
                record.timestamp()
        );
    }

    private Date convertToTargetTime(String timestampStr) {
        Instant instant = Instant.parse(timestampStr);
        ZonedDateTime zonedTime = instant.atZone(targetZone);
        return Date.from(zonedTime.toInstant());
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}