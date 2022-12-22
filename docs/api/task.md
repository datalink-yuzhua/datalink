# 提供的http接口

### 示例

- 请求地址：

  GET `/v2/tasks`
- 请求参数：
    - header:

      无
    - query:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|desc|string|-|任务名称|否|
|state|enum:null,init,done,stop,running,poor|-|任务状态|否|
|page|integer|-|当前第几页|是|
|size|integer|-|每页大小|是|

    - body:

      无

- 返回参数：

```json5
{}
```

- 其他说明：

  无

## 资源(resource)

### 检查资源有效性

- 请求地址：

  GET `/resource/ping`
- 请求参数：
    - header:

      无
    - query:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|type|enum:mysql,mongodb,elasticsearch|-|数据源类型|是|
|user|string|-|账户|否|
|pass|string|-|密码|否|
|host|string|-|服务host|否|
|port|string|-|端口|否|
|dsn|string|-|数据源名称|否|

    - body:

      无

- 返回参数：

```json5
{
  "code": "10000",
  "data": {
    "desc": "",
    "dsn": "",
    "extra": null,
    "host": "http://db-01.test.com",
    // 检测成功之后,会返回id.
    "id": "18126dee-3d78-42a4-a24f-ec78b524b9fb",
    "pass": "pass",
    "port": "80",
    "type": "elasticsearch",
    "user": "elastic"
  },
  "msg": "success"
}
```

- 其他说明：

  dsn 与 (user pass host port) 需要填写一种.都填写时,优先dsn.  
  当检测成功时会返回id,id为资源唯一id,在task全局使用

### 资源文档集

- 请求地址：

  PUT `/resource/document-sets`
- 请求参数：
    - header:

      `Content-Type: application/json`
    - query:

      无
    - body:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|id|string|-|id|是|
|type|enum:mysql|-|数据源类型,目前只支持mysql|是|
|user|string|-|账户|否|
|pass|string|-|密码|否|
|host|string|-|服务host|否|
|port|string|-|端口|否|
|dsn|string|-|数据源名称|否|

- 返回参数：

```json5
{
  "code": "10000",
  "data": [
    {
      // 数据别名,当为es时,别名为 index
      "alias": "database",
      // 一级分类
      "layer": "top",
      // 数据库名称
      "name": "test",
      // 当为es时,sub为空
      "sub": [
        {
          // 数据别名,当为MongoDB时,别名为collection
          "alias": "table",
          "cols": {
            "cate": {
              "field": "cate",
              "key": "",
              "type": "int(11)"
            },
            "code": {
              "field": "code",
              "key": "",
              "type": "varchar(10)"
            },
            "country_name": {
              "field": "country_name",
              "key": "",
              "type": "varchar(100)"
            },
            "id": {
              "field": "id",
              "key": "PRI",
              "type": "int(10) unsigned"
            },
            "name": {
              "field": "name",
              "key": "",
              "type": "varchar(255)"
            }
          },
          // 二级分类
          "layer": "second",
          // 表名
          "name": "country"
        }
      ]
    }
  ],
  "msg": "success"
}
```

- 其他说明：

    1. 数据源为MySQL时会忽略数据库:information_schema,performance_schema,mysql
    2. 目前只支持MySQL,Elasticsearch

## 任务(task)

### 新建任务

- 请求地址：

  PUT `/task`
- 请求参数：
    - header:

      `Content-Type: application/json`
    - query:

      无
    - body:

```json5
{
  // 基本设置
  "setup": {
    // 任务描述|任务名称
    "desc": "from mysql to elasticsearch",
    // 开启错误记录
    "error_record": true
  },
  // 资源列表
  "resource": [
    {
      // 资源连接信息
      // 该部分数据从 /resource/ping 接口回填
      "id": "a9e96c08-56de-4d97-a504-59ddaccee8c6",
      "type": "mysql",
      "user": "root",
      "pass": "root",
      "host": "127.0.0.1",
      "port": "3306",
      "dsn": ""
    },
    {
      "extra": null,
      "host": "db-01.test.com",
      "id": "bf907d93-e499-449f-92f2-cd2f057450bc",
      "pass": "pass",
      "port": "80",
      "type": "elasticsearch",
      "dsn": "",
      "user": "elastic"
    }
  ],
  // 读取数据源,或者关联数据源.
  "source": [
    {
      // 资源id
      "resource_id": "a9e96c08-56de-4d97-a504-59ddaccee8c6",
      // 同步模式:direct,stream,replica,empty
      "sync_mode": "direct",
      // 文档集,在MySQL中即为,那个数据库中的那个表
      "document_set": "test.country",
      "extra": {
        // 其他设置项
      }
    }
  ],
  // 写入数据源
  "target": [
    {
      // 资源id
      "resource_id": "bf907d93-e499-449f-92f2-cd2f057450bc",
      // 文档映射关系
      "document_set": {
        // 源数据 => 目标数据集,在elasticsearch中为索引名称
        "test.country": "test_country_001"
      },
      "extra": {
      }
    }
  ],
  // 管道处理单元
  "pipeline": [
    {
      // 处理的数据集
      "document_set": "test.country",
      // 处理单元集
      "flow": [
        // 处理单元,也是执行顺序
        {
          // map,主要是文档变换
          "type": "map",
          // js,返回文档
          "script": "module.exports=function(doc){doc.attr1=2022;return doc;}"
        },
        {
          // 过滤,函数返回true就过滤文档,false不过滤文档
          "type": "filter",
          // js,返回bool值
          "script": "module.exports=function(doc){return doc.id>100;}"
        },
        {
          // 字段映射,有一小部分map功能.只是改变字符安名称,建议改方法.
          "type": "mapInline",
          "field_map": [
            {
              // 源数据字段名称
              "srcField": "id",
              "srcType": "long",
              // 目标数据字段名称
              "aimField": "tab_id",
              "aimType": "long"
            },
            {
              "srcField": "code",
              "srcType": "string",
              "aimField": "tab_code",
              "aimType": "string"
            },
            {
              "srcField": "name",
              "srcType": "string",
              "aimField": "tab_name",
              "aimType": "string"
            }
          ]
        },
        {
          // 关联数据,使用的事建立连接查询表数据,性能较差
          // 目前数据源:MySQL,MongoDB两种支持关联数据
          "type": "relateInline",
          // 关联类型 11,1n
          "assoc_type": "11",
          // 预留
          "script": "",
          // 关联资源
          "relate_resource_id": "c888b053-45f3-4c98-b219-8acfe67999d1",
          // 文档集,当为MySQL为:database.table
          "relate_document_set": "test.country_cate",
          // 关联文档与当前文档层级关系:sib 同级, sub子级
          "layer_type": "sib",
          // 如果为子级,键的名称
          "sub_label": "",
          // 同级时,字段映射
          "field_map": {
            // r.id => r 为关联数据集,在当前文档中时名字为 cate_id
            "r.id": "cate_id"
          },
          // 关联条件,目前只支持 = 
          "wheres": [
            {
              // 源字段
              "src_field": "cate",
              // 逻辑关系
              "operator": "=",
              // 目标字段
              "rel_field": "id"
            }
          ]
        }
      ]
    }
  ]
}
```

- 返回参数：

```json5
{
  "code": "10000",
  "data": {
    "notice": "add task success,run task need POST!",
    "taskId": "efd3309c-693c-11ec-918e-00ffb182f508"
  },
  "msg": "success"
}
```

- 其他说明：

  任务创建成功之后,需要启动.

### 任务列表

- 请求地址：

  GET `/v2/tasks`
- 请求参数：
    - header:

      无
    - query:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|desc|string|-|任务名称|否|
|state|enum:null,init,done,stop,running,poor|-|任务状态|否|
|page|integer|-|当前第几页|是|
|size|integer|-|每页大小|是|

    - body:

      无

- 返回参数：

```json5

```

- 其他说明： 无

### 启动任务|停止任务

- 请求地址：

  POST `/task`
- 请求参数：
    - header:

      `Content-Type: application/json`
    - query:

      无
    - body:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|id|string|-|任务id|否|
|op|enum:start,stop|-|任务操作|否|

- 返回参数：

```json5
{
  "code": "10000",
  "data": true,
  "msg": "success"
}
```

- 其他说明：

  无

### 删除任务

- 请求地址：

  DELETE `/task`
- 请求参数：
    - header:

      `Content-Type: application/json`
    - query:

      无
    - body:

|字段|类型|默认值|备注|是否必填|
|---|---|---|---|---|
|id|string|-|任务id|否|

- 返回参数：

```json5
{
  "code": "10000",
  "data": true,
  "msg": "success"
}
```

- 其他说明：

  无
