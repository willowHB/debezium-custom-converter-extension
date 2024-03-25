package com.willowtech.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * 处理Debezium时间转换的问题：全部转成timestamp
 * Debezium默认将MySQL中datetime类型转成UTC的时间戳({@link io.debezium.time.Timestamp})，时区是写死的没法儿改，
 * 导致数据库中设置的UTC+8，到kafka中变成了多八个小时的long型时间戳
 * Debezium默认将MySQL中的timestamp类型转成UTC的字符串。
 * | mysql                               | mysql-binlog-connector                   | debezium                          |
 * | ----------------------------------- | ---------------------------------------- | --------------------------------- |
 * | date<br>(2021-01-28)                | LocalDate<br/>(2021-01-28)               | Integer<br/>(18655)               |
 * | time<br/>(17:29:04)                 | Duration<br/>(PT17H29M4S)                | Long<br/>(62944000000)            |
 * | timestamp<br/>(2021-01-28 17:29:04) | ZonedDateTime<br/>(2021-01-28T09:29:04Z) | String<br/>(2021-01-28T09:29:04Z) |
 * | Datetime<br/>(2021-01-28 17:29:04)  | LocalDateTime<br/>(2021-01-28T17:29:04)  | Long<br/>(1611854944000)          |
 *
 * @see io.debezium.connector.mysql.converters.TinyIntOneToBooleanConverter
 */
@Slf4j
public class MySqlDateTime2TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            log.error("The \"{}\" setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.int64().optional().name("com.willowtech.debezium.date.int64");
            converter = this::convertDate;
        }
        if ("TIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.int64().optional().name("com.willowtech.debezium.time.int64");
            converter = this::convertTime;
        }
        if ("DATETIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.int64().optional().name("com.willowtech.debezium.datetime.int64");
            converter = this::convertDateTime;
        }
        if ("TIMESTAMP".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.int64().optional().name("com.willowtech.debezium.timestamp.int64");
            converter = this::convertTimestamp;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            log.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }
    }

    private Long convertDate(Object input) {
        if (input instanceof LocalDate) {

            LocalDate localDate = (LocalDate) input;
            return localDate.atStartOfDay(ZoneOffset.of(timestampZoneId.getId())).toInstant().toEpochMilli();

        }
        if (input instanceof Integer) {
            LocalDate localDate = LocalDate.ofEpochDay((Integer) input);
            return localDate.atStartOfDay(ZoneOffset.of(timestampZoneId.getId())).toInstant().toEpochMilli();
        }
        return null;
    }

    private Long convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            return seconds * 1000;
        }
        return null;
    }

    /**
     * DateTime转成时间戳long类型
     *
     * @param input
     * @return
     */
    public Long convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            LocalDateTime localDateTime = (LocalDateTime) input;
            return localDateTime.toInstant(ZoneOffset.of(timestampZoneId.getId())).toEpochMilli();
        }
        return null;
    }

    private Long convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            // mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime = zonedDateTime.toLocalDateTime();
            return localDateTime.toInstant(ZoneOffset.of(timestampZoneId.getId())).toEpochMilli();
        }
        return null;
    }

}
