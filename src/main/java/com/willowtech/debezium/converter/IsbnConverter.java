package com.willowtech.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import java.util.Properties;

/**
 * 官方举例：
 * url：https://debezium.io/documentation/reference/1.9/development/converters.html#custom-converters
 * <p>
 * 配置：
 * converters: isbn
 * isbn.type: com.willowtech.debezium.converter.IsbnConverter
 * isbn.schema.name: io.debezium.postgresql.type.Isbn
 *
 * @author hb
 * @version V1.0
 * @date 2024
 */
public class IsbnConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private SchemaBuilder isbnSchema;

    @Override
    public void configure(Properties props) {
        isbnSchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(RelationalColumn column,
                             CustomConverter.ConverterRegistration<SchemaBuilder> registration) {

        if ("isbn".equals(column.typeName())) {
            registration.register(isbnSchema, x -> x.toString());
        }
    }
}
