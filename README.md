> [参考大牛的源码](https://github.com/holmofy/debezium-datetime-converter#readme)
[![Debezium architecture](https://debezium.io/documentation/reference/1.9/_images/debezium-architecture.png)](https://debezium.io/documentation/reference/1.9/connectors/mysql.html)

[![Build Status(https://github.com/holmofy/debezium-datetime-converter/actions/workflows/release.yml/badge.svg)](https://github.com/holmofy/debezium-datetime-converter/actions/workflows/release.yml/badge.svg)](https://github.com/holmofy/debezium-datetime-converter/releases)

# debezium-datetime-converter

Debezium [custom converter](https://debezium.io/documentation/reference/development/converters.html) is used to deal
with
mysql [datetime type problems](https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-temporal-types)
.

| mysql                               | binlog-connector                         | debezium                          | schema                 |
| ----------------------------------- | ---------------------------------------- | --------------------------------- | ----------------------------------- |
| date<br>(2021-01-28)                | LocalDate<br/>(2021-01-28)               | Integer<br/>(18655)               | io.debezium.time.Date               |
| time<br/>(17:29:04)                 | Duration<br/>(PT17H29M4S)                | Long<br/>(62944000000)            | io.debezium.time.Time               |
| timestamp<br/>(2021-01-28 17:29:04) | ZonedDateTime<br/>(2021-01-28T09:29:04Z) | String<br/>(2021-01-28T09:29:04Z) | io.debezium.time.ZonedTimestamp     |
| Datetime<br/>(2021-01-28 17:29:04)  | LocalDateTime<br/>(2021-01-28T17:29:04)  | Long<br/>(1611854944000)          | io.debezium.time.Timestamp          |

> For details, please refer to [this article](https://blog.hufeifei.cn/2021/03/13/DB/mysql-binlog-parser/)
>
> <font color=#00ffff size=4>在此基础上，我对代码进行了改造，我需要把MySql的Datetime类型转成时间戳，即long类型数值，然后存入es.(其他类型可以自行修改)</font>

# Usage
###### 0. 官方提供：IsbnConverter,TinyIntOneToBooleanConverter可供学习参考！！！

###### 1. debezium-datetime-converter 自定义转换器使用方法
[Download](https://github.com/holmofy/debezium-datetime-converter/releases) the extended jar package and put it in
   the same level directory of the debezium plugin.或者debezium的lib目录下，当然debezium本身也是在plugin目录下的。

###### 2. 时间转换成string类型的格式：MySqlDateTimeConverter
In debezium-connector,如果想把时间转换成string类型的格式, Add the following configuration:

```properties
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
# ...
"converters": "datetime",
"datetime.type": "MySqlDateTimeConverter",
"datetime.format": "yyyy-MM-dd",
"datetime.format.time": "HH:mm:ss",
"datetime.format.datetime": "yyyy-MM-dd HH:mm:ss",
"datetime.format.timestamp": "yyyy-MM-dd HH:mm:ss",
"datetime.format.timestamp.zone": "UTC+8"
```

###### 3. 时间转换成timestamp：MySqlDateTime2TimestampConverter
In debezium-connector, 如果想把时间转换成timestamp,Add the following configuration:
去掉了时区的影响，根据配置的时区，转换后的long类型时间无时区影响
```properties
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
# ...
"converters": "datetime",
"datetime.type": "MySqlDateTime2TimestampConverter",
"datetime.format": "yyyy-MM-dd",
"datetime.format.time": "HH:mm:ss",
"datetime.format.datetime": "yyyy-MM-dd HH:mm:ss",
"datetime.format.timestamp": "yyyy-MM-dd HH:mm:ss",
"datetime.format.timestamp.zone": "+08:00"
```

###### 4. String转Array：JsonString2ObjectConverter
String转Array,支持将 '[111111111,2222222222222]' 转为  ["111111111","2222222222222"] ;或者将 '["aaaaaa","ccccccccccccc"]' 转为  ["aaaaaa","ccccccccccccc"]
```properties
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
# ...
"converters": "jsonextract",
"jsonextract.type": "JsonString2ObjectConverter",
"jsonextract.transfer.field": "column_field"
```
###### 5，String转Array：ES自带的Ingest Pipeline处理
当然，这个String转Array的需求也可以在mysql数据库存储，'aaa,bbb,ccc'这样的字符串，然后使用ES自带的Ingest Pipeline处理，方便快捷：
```properties
PUT _ingest/pipeline/string_to_array_pipeline
{
"description": "Transfer the string which is concat with a separtor  to array.",
"processors": [
{
"split": {
"field": "field1",
"separator": ","
}
},
{
"split": {
"field": "field2",
"separator": ","
}
}
]
}

PUT index/_settings
{
"default_pipeline": "string_to_array_pipeline"
}
```
###### 6，[使用过程中，更多细节请参考这篇文章](https://www.cnblogs.com/hbuuid/p/18093930)
