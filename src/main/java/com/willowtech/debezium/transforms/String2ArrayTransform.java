package com.willowtech.debezium.transforms;

import com.willowtech.debezium.util.NonEmptyListValidator;
import com.willowtech.debezium.util.SimpleConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.willowtech.debezium.util.Requirements;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Kafka-connect自定义Transformation
 * 1、convertToArray方法返回String[]数组,即把原始字符串转成数组,转换失败
 * 错误信息：Invalid Java object for schema with type STRING: class [Ljava.lang.String; for field: "num_array"
 * 2、convertToArray方法返回String[]数组,即把原始字符串转成List集合,转换失败
 * 错误信息：Invalid Java object for schema with type STRING: class java.util.Arrays$ArrayList for field: "num_array"
 *
 * @author lhb
 * @date 2024/3/19
 * @since 1.0.0
 */
public class String2ArrayTransform<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(String2ArrayTransform.class);
    private static final String PURPOSE = "extract fields from string to array";

    public static final String FIELDS_CONFIG = "fields";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field names on the record value to extract to Array.");
    /**
     * 在配置中使用的字段名
     */
    private List<String> fields;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
    }

    @Override
    public R apply(R record) {

        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        //
        log.info("applyWithSchema =================================");
        log.info("配置的fields = {}", fields);
        // 获取记录的值
        //SourceRecord{sourcePartition={server=goods}, sourceOffset={transaction_id=null, ts_sec=1710816449, file=mysql-bin.000080, pos=16948, row=1, server_id=1,
        // event=2}} ConnectRecord{topic='goods.goods.t_mountain', kafkaPartition=null, key=Struct{id=110}, keySchema=Schema{goods.goods.t_mountain.Key:STRUCT},
        // value=Struct{after=Struct{id=110,name=少华山,location=34.497647,110.073028,latitude=34.497647,logtitude=110.073028,altitude=1200.00,create_user=111111,
        // create_time=1710419563000,update_time=1710420074000,ticket=0.00,desc=少华山在陕西渭南华州区,num_array=["100","200"]},source=Struct{version=1.9.4.Final,
        // connector=mysql,name=goods,ts_ms=1710816449000,db=goods,table=t_mountain,server_id=1,file=mysql-bin.000080,pos=17105,row=0,thread=118},op=c,
        // ts_ms=1710821872668}, valueSchema=Schema{goods.goods.t_mountain.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        log.info("要处理的record：{}", record);

        final Struct value = Requirements.requireStruct(record.value(), PURPOSE);
        //Struct{after=Struct{id=110,name=少华山,location=34.497647,110.073028,latitude=34.497647,logtitude=110.073028,altitude=1200.00,
        // create_user=111111,create_time=1710419563000,update_time=1710420074000,ticket=0.00,desc=少华山在陕西渭南华州区,num_array=["100","200"]},
        // source=Struct{version=1.9.4.Final,connector=mysql,name=goods,ts_ms=1710816449000,db=goods,table=t_mountain,server_id=1,
        // file=mysql-bin.000080,pos=17105,row=0,thread=118},op=c,ts_ms=1710821872668}

        log.info("要处理的字段value：{}", value);

        Struct after = value.getStruct("after");
        log.info("要处理的字段after：{}", after);

        Field fieldAfter = (Field) value.schema().fields().get(1);
        log.info("要处理的字段fieldAfter：{}", fieldAfter);

        // 获取记录的schema
        //Schema{goods.goods.t_mountain.Envelope:STRUCT}
        Schema schema = value.schema();
        log.info("要处理的字段schema：{}", schema);
        List<Field> fields = schema.fields();
        log.info("要处理的记录fields：{}", fields);

//        final SchemaBuilder keySchemaBuilder = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().name("com.willowtech.debezium.array");  ----> Not a struct schema: Schema{com.willowtech.debezium.array:ARRAY}
        final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct(); //todo 想办法把Array设置进去看看情况
        for (String field : this.fields) {
            Object fieldValue = after.get(field);
            //num_array
            log.info("要处理的记录字段：{}，原始值为：{}", field, fieldValue);
            // 根据字段类型转换为数组
            List<String> newValue = convertToArray(fieldValue.toString());
            log.info("处理后的值为：{}", newValue);
            // 更新值字段

            Field newField = new Field(fieldAfter.name(), fieldAfter.index(), keySchemaBuilder.build());
            log.info("处理后newField：{}", newField);

//            keySchemaBuilder.field(field, fieldAfter.schema());
            Schema keySchema = keySchemaBuilder.build();
            final Struct key = new Struct(keySchema);

            after.put(field, newValue);

            key.put(newField, after);
        }
        log.info("处理后record：{}", record);

        return record;
        //        return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, value.schema(), value, record.timestamp());
    }

    private R applySchemaless(R record) {
        log.info("配置的fields = {}", fields);
        // 获取记录的值
        //SourceRecord{sourcePartition={server=goods}, sourceOffset={transaction_id=null, ts_sec=1710816449, file=mysql-bin.000080, pos=16948, row=1, server_id=1,
        // event=2}} ConnectRecord{topic='goods.goods.t_mountain', kafkaPartition=null, key=Struct{id=110}, keySchema=Schema{goods.goods.t_mountain.Key:STRUCT},
        // value=Struct{after=Struct{id=110,name=少华山,location=34.497647,110.073028,latitude=34.497647,logtitude=110.073028,altitude=1200.00,create_user=111111,
        // create_time=1710419563000,update_time=1710420074000,ticket=0.00,desc=少华山在陕西渭南华州区,num_array=["100","200"]},source=Struct{version=1.9.4.Final,
        // connector=mysql,name=goods,ts_ms=1710816449000,db=goods,table=t_mountain,server_id=1,file=mysql-bin.000080,pos=17105,row=0,thread=118},op=c,
        // ts_ms=1710821872668}, valueSchema=Schema{goods.goods.t_mountain.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        log.info("要处理的record：{}", record);

        final Struct value = Requirements.requireStruct(record.value(), PURPOSE);
        //Struct{after=Struct{id=110,name=少华山,location=34.497647,110.073028,latitude=34.497647,logtitude=110.073028,altitude=1200.00,
        // create_user=111111,create_time=1710419563000,update_time=1710420074000,ticket=0.00,desc=少华山在陕西渭南华州区,num_array=["100","200"]},
        // source=Struct{version=1.9.4.Final,connector=mysql,name=goods,ts_ms=1710816449000,db=goods,table=t_mountain,server_id=1,
        // file=mysql-bin.000080,pos=17105,row=0,thread=118},op=c,ts_ms=1710821872668}

        log.info("要处理的字段value：{}", value);

        // 获取记录的schema
        //Schema{goods.goods.t_mountain.Envelope:STRUCT}
        Schema schema = value.schema();
        log.info("要处理的字段schema：{}", schema);

        final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
        for (String field : fields) {
            //num_array
            log.info("要处理的字段fieldName：{}", field);
        }
        return null;
    }

    /**
     * 实现将原始值转换为数组的逻辑
     *
     * @param originalValue
     * @return
     */
    private List<String> convertToArray(String originalValue) {
        // 这里是转换逻辑，具体取决于originalValue的类型
        // 例如，如果originalValue是一个String，你可以创建一个只有一个元素的数组
        // 如果originalValue已经是一个数组，你可能需要将其转换为一个新的数组类型
        // 如果originalValue是一个集合，你可以将其转换为一个数组
        // ...
        String[] array = new String[0];
        log.info("String 转成 数组 ，原始值为{}", originalValue);
        // 假设originalValue是一个String，我们将其转换为一个String数组
        try {
            array = objectMapper.readValue(originalValue, String[].class);
        } catch (JsonProcessingException e) {
            log.info("转换异常", e);
            e.printStackTrace();
        }
        return Arrays.asList(array);
    }

    @Override
    public ConfigDef config() {
        // 返回转换器所需的配置定义
        // 这里你可以定义你的转换器所需要的配置参数，比如字段名
        return new ConfigDef()
                .define(FIELDS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The name of the field to convert to an array.");
    }

    @Override
    public void close() {
        // 在转换器关闭时执行任何必要的清理操作
    }
}