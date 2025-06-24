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

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field.name", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "Name of the field containing the timestamp to convert")
            .define("timezone", ConfigDef.Type.STRING, DEFAULT_ZONE.getId(),
                    ConfigDef.Importance.MEDIUM, "Target timezone ID")
            .define("input.format", ConfigDef.Type.STRING, "auto",
                    ConfigDef.Importance.MEDIUM, "Input format: 'iso8601', 'epoch_micro', or 'auto'");

    private String fieldName;
    private ZoneId targetZone;
    private String inputFormat;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldName = config.getString("field.name");
        this.targetZone = ZoneId.of(config.getString("timezone"));
        this.inputFormat = config.getString("input.format");
        log.info("Configured to convert field '{}' (format: {}) to timezone {}",
                fieldName, inputFormat, targetZone);
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
                    Date converted = convertTimestamp(after.get(field));
                    newStruct.put(field.name(), converted);
                    log.debug("Converted {} from {} to {}", fieldName,
                            after.get(field), converted);
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

    private Date convertTimestamp(Object timestampValue) {
        if (timestampValue == null) {
            return null;
        }

        Instant instant;

        // Handle different input formats
        if ("iso8601".equalsIgnoreCase(inputFormat) ||
                ("auto".equalsIgnoreCase(inputFormat) && timestampValue instanceof String)) {
            // ISO 8601 string format
            instant = Instant.parse(timestampValue.toString());
        } else if ("epoch_micro".equalsIgnoreCase(inputFormat) ||
                ("auto".equalsIgnoreCase(inputFormat) && timestampValue instanceof Number)) {
            // Epoch microseconds (like 1697546114548325)
            long micros = ((Number) timestampValue).longValue();
            long seconds = micros / 1_000_000;
            long nanos = (micros % 1_000_000) * 1_000;
            instant = Instant.ofEpochSecond(seconds, nanos);
        } else {
            throw new DataException("Unsupported timestamp format or type for value: " + timestampValue);
        }

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