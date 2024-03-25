package com.willowtech.debezium.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * DEBEZIUM自定义Converter
 * 1、convertJsonString2Array方法返回String[],即把原始字符串转成数组,转换失败
 * 错误信息：Invalid Java object for schema "com.willowtech.debezium.varchar.array" with type ARRAY: class [Ljava.lang.String; for field: "num_array"
 * 2、convertJsonString2Array方法返回List<String>,即把原始字符串转成List集合，转换成功！！！！！！！！
 * --支持将 '[111111111,2222222222222]' 转为  ["111111111","2222222222222"]
 * --支持将 '["aaaaaa","ccccccccccccc"]' 转为  ["aaaaaa","ccccccccccccc"]
 *
 * @author lhb
 * @date 2024/3/15
 * @since 1.0.0
 */
@Slf4j
public class JsonString2ObjectConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    /**
     * 在配置中使用的字段名
     */
    private String fieldName;
    private static final String SETTING_KEY = "transfer.field";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Properties props) {
        // 从配置中获取字段名
        fieldName = (String) props.get(SETTING_KEY);
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {

        if (field.name().equals(fieldName)) {
            String sqlType = field.typeName().toUpperCase();

            SchemaBuilder schemaBuilder = null;
            Converter converter = null;
            if ("VARCHAR".equals(sqlType)) {
                //一维字符串数组对象
                schemaBuilder = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().name("com.willowtech.debezium.varchar.array");
                converter = this::convertJsonString2Array;
            }

            if (schemaBuilder != null) {
                registration.register(schemaBuilder, converter);
                log.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
            }
        }

    }

    private List<String> convertJsonString2Array(Object input) {
        if (input == null) {
            return null;
        }
        if (input instanceof byte[]) {
            byte[] bytes = (byte[]) input;
            //记录一下：
            //时间：2024年3月21日17:03:54
            //原始字段存储的如果是中文，下面这个转换方法，无法正确还原，后面有时间了在研究一下中文编码的问题
            //所以，这个需求还是交给es自己去处理比较好，使用ingest pipeline 中中的split方法完美解决。本例中有介绍！
            String originalString = new String(bytes, StandardCharsets.UTF_16);
            String[] array = new String[0];
            try {
                array = objectMapper.readValue(originalString, String[].class);
            } catch (JsonProcessingException e) {
                log.error("byte transfer error", e);
            }
            return Arrays.asList(array);
        }

        if (input instanceof String) {
            String inputString = (String) input;
            try {
                // 将去除开始和结束的引号后的字符串转换为JSON
                JsonNode jsonNode = objectMapper.readTree(inputString.substring(1, inputString.length() - 1));
                // 输出去掉引号后的JSON
                String str = jsonNode.toString();
                return Arrays.asList(objectMapper.readValue(str, String[].class));
            } catch (JsonProcessingException e) {
                log.error("String transfer error", e);
            }
        }
        return new ArrayList<>();
    }

}
