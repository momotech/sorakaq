# HTTP接口
- 请求方法 POST
- 请求Body及返回值均为json
## 1.新增topic（创建topic，并给topic分配worker列表）
```
url: ${SORAKAQ-ADMIN}:9087/sorakaq/topic/add （端口与admin配置有关）
```
### 参数

| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| string |business |使用场景|
| string |topic_name|主题名称 |
| int    |worker_num|分配的worker数 |

### 返回值
```
{"ec":200,"em":"success","result":true}
```
| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| int          |ec |错误码|
| string       |em |错误信息 |
| bool         |result|true或者false |

### 测试

```
curl http://${SORAKAQ-ADMIN}:9087/sorakaq/topic/add -H "Content-Type: application/json" -X POST  --data '{"business": "test", "topic_name":"delay_test", "worker_num": 1}'
```

## 2.更新topic（给topic重新分配worker列表）
```
url: ${SORAKAQ-ADMIN}:9087/sorakaq/topic/update （端口与admin配置有关）
```
### 参数

| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| string |topic_name|主题名称 |
| int    |worker_num|分配的worker数 |

### 返回值
```
{"ec":200,"em":"success","result":true}
```
| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| int          |ec |错误码|
| string       |em |错误信息 |
| bool         |result|true或者false |

### 测试

```
curl http://${SORAKAQ-ADMIN}:9087/sorakaq/topic/update -H "Content-Type: application/json" -X POST  --data '{"topic_name":"delay_test", "worker_num": 1}'
```

## 3.消息写入
```
url: ${SORAKAQ-RECEIVE}:9088/sorakaq/msg/submit （端口与receive配置有关）
```
### 参数

| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| string |topic_name  |主题名称 |
| uint64 |delay_time  |毫秒延时时间(如果延时时间小于100ms，直接回调) |
| int    |ttr         |毫秒回调超时时间(默认1000ms) |
| string |request_type|回调请求类型(POST或者GET) |
| string |url         |回调url |
| string |params      |回调参数 |

### 返回值
```
{"ec":200,"em":"success","result":{"msgId":"e8fec0ac-1dfb-476d-8f59-f4cfeadf6f1f"}}
```
| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| int          |ec |错误码|
| string       |em |错误信息 |
| string       |msgId|消息id|

### 测试

```
curl http://${SORAKAQ-RECEIVE}:9088/sorakaq/msg/submit -H "Content-Type: application/json" -X POST  --data '{"topic_name":"delay_test2", "delay_time":10000,"ttr":1000,"callback":{"request_type":"POST","url":"http://127.0.0.1//test.php","params":"{\"id\":111}"}}'
```

## 4.消息撤回
```
url: ${SORAKAQ-RECEIVE}:9088/sorakaq/msg/cancel （端口与receive配置有关）
```
### 参数

| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| string |topic_name|主题名称 |
| string |msg_id|消息id |


### 返回值
```
{"ec":200,"em":"success","result":true}
```
| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| int          |ec |错误码|
| string       |em |错误信息 |
| bool         |result| true或者false|

### 测试

```
curl http://${SORAKAQ-RECEIVE}:9088/sorakaq/msg/cancel -H "Content-Type: application/json" -X POST  --data '{"topic_name":"delay_test", "msg_id": "e8fec0ac-1dfb-476d-8f59-f4cfeadf6f1f"}'
```

## 5.消息加时
```
url: ${SORAKAQ-RECEIVE}:9088/sorakaq/msg/addTime （端口与receive配置有关）
```
### 参数

| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| string |topic_name|主题名称 |
| string |msg_id|消息id |
| uint64 |add_time    |毫秒加时时间 |

### 返回值
```
{"ec":200,"em":"success","result":true}
```
| 类型| 参数 | 说明 |
| ------------ | ------------- | ------------ |
| int          |ec |错误码|
| string       |em |错误信息 |
| bool         |result| true或者false|

### 测试

```
curl http://${SORAKAQ-RECEIVE}:9088/sorakaq/msg/addTime -H "Content-Type: application/json" -X POST  --data '{"topic_name":"delay_test", "msg_id": "e8fec0ac-1dfb-476d-8f59-f4cfeadf6f1f", "add_time": 2000}'
```