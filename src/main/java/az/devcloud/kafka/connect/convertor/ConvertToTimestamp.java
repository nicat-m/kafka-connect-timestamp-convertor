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
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

public class ConvertToTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ConvertToTimestamp.class);
    private static final ZoneId DEFAULT_ZONE = ZoneId.of("Asia/Baku");

    public static final String MISSING_OR_INVALID_SKIP = "skip";
    public static final String MISSING_OR_INVALID_FAIL = "fail";
    public static final String MISSING_OR_INVALID_DEFAULT = "fail";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("field.names", ConfigDef.Type.LIST, Collections.emptyList(),
                    ConfigDef.Importance.HIGH, "Comma-separated list of fields containing timestamps to convert")
            .define("timezone", ConfigDef.Type.STRING, DEFAULT_ZONE.getId(),
                    ConfigDef.Importance.MEDIUM, "Target timezone ID")
            .define("input.format", ConfigDef.Type.STRING, "auto",
                    ConfigDef.Importance.MEDIUM, "Input format: 'iso8601', 'epoch_micro', or 'auto'")
            .define("missing.or.invalid.treatment", ConfigDef.Type.STRING, MISSING_OR_INVALID_DEFAULT,
                    ConfigDef.ValidString.in(MISSING_OR_INVALID_SKIP, MISSING_OR_INVALID_FAIL),
                    ConfigDef.Importance.MEDIUM, "How to handle missing or invalid fields: 'skip' or 'fail'");

    private List<String> fieldNames;
    private ZoneId targetZone;
    private String inputFormat;
    private String missingOrInvalidTreatment;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldNames = config.getList("field.names");
        this.targetZone = ZoneId.of(config.getString("timezone"));
        this.inputFormat = config.getString("input.format");
        this.missingOrInvalidTreatment = config.getString("missing.or.invalid.treatment");
        log.info("Configured to convert fields {} (format: {}) to timezone {}. Missing/invalid treatment: {}",
                fieldNames, inputFormat, targetZone, missingOrInvalidTreatment);
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

        // Validate fields exist
        List<String> missingFields = fieldNames.stream()
                .filter(f -> after.schema().field(f) == null)
                .collect(Collectors.toList());

        if (!missingFields.isEmpty()) {
            if (MISSING_OR_INVALID_SKIP.equals(missingOrInvalidTreatment)) {
                log.warn("Skipping missing fields: {}", missingFields);
                fieldNames.removeAll(missingFields);
            } else {
                throw new DataException("Fields not found in record: " + missingFields);
            }
        }

        // Build new schema with timestamp types for specified fields
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Field field : after.schema().fields()) {
            if (fieldNames.contains(field.name())) {
                // Create optional timestamp schema
                schemaBuilder.field(field.name(), Timestamp.builder().optional().build());
            } else {
                schemaBuilder.field(field.name(), field.schema());
            }
        }

        // Create new struct with converted timestamps
        Struct newStruct = new Struct(schemaBuilder.build());
        for (Field field : after.schema().fields()) {
            if (fieldNames.contains(field.name())) {
                try {
                    Object fieldValue = after.get(field);
                    if (fieldValue == null) {
                        if (MISSING_OR_INVALID_SKIP.equals(missingOrInvalidTreatment)) {
                            log.debug("Field {} is null - skipping conversion", field.name());
                            newStruct.put(field.name(), null);
                            continue;
                        } else {
                            throw new DataException("Null value for timestamp field: " + field.name());
                        }
                    }

                    Date converted = convertTimestamp(fieldValue);
                    newStruct.put(field.name(), converted);
                    log.debug("Converted {} from {} to {}", field.name(), fieldValue, converted);
                } catch (Exception e) {
                    if (MISSING_OR_INVALID_SKIP.equals(missingOrInvalidTreatment)) {
                        log.warn("Skipping invalid timestamp conversion for field {}: {}", field.name(), e.getMessage());
                        newStruct.put(field.name(), null);
                    } else {
                        throw new DataException("Failed to convert timestamp field: " + field.name(), e);
                    }
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