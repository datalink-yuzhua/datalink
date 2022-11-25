package linkd

import (
	"data-link-2.0/datalink/loop"
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/cst"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	elastic7 "github.com/olivere/elastic/v7"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
)

// 用于输入文件的校验

// ValidTaskMap 校验输入字符串
func ValidTaskMap(c map[string]interface{}) error {

	// 检查setup部分
	err := ValidSetup(c)
	if err != nil {
		return err
	}

	// 检查resource部分
	conn, err := ValidConnects(c)
	if err != nil {
		return err
	}

	// 检查source部分
	srcMap, err := ValidSource(c, conn)
	if err != nil {
		return err
	}

	// 检查target部分
	err = ValidTarget(c, conn)
	if err != nil {
		return err
	}

	// 检查pipeline部分
	err = ValidPipeline(c, srcMap)
	if err != nil {
		return err
	}
	return nil
}

// ValidSetup 校验设置
func ValidSetup(s map[string]interface{}) error {
	c, ok := GetMapSI(s, "setup")
	if !ok {
		return errors.New("需要setup部分")
	}
	desc, _ := GetMapString(c, "desc")
	if desc == "" {
		return errors.New("需要setup->desc部分")
	}
	_, ok = GetMapBool(c, "error_record")
	if !ok {
		return errors.New("需要setup->error_record部分")
	}
	return nil
}

// ValidConnects 校验服务是否可以连接
func ValidConnects(c map[string]interface{}) (map[string]map[string]interface{}, error) {
	arr, ok := GetMapArr(c, "resource")
	if !ok {
		return nil, errors.New("需要resource部分")
	}
	// 检查 no1000001 不重复,type 不为空
	idMap := map[string]map[string]interface{}{}
	for _, v := range arr {
		var _id string
		r := map[string]interface{}{}

		// 1.检查基础值,转换值
		switch v := v.(type) {
		case map[string]interface{}:
			for k, v1 := range v {
				v2, ok := v1.(string)
				if !ok {
					continue
				}
				switch k {
				case "id":
					_, ok := idMap[v2]
					if ok {
						return nil, errors.New("resource:id 必须唯一")
					}
					_id = v2
				case "type":
					if !cst.InResourceType(v2) {
						return nil, fmt.Errorf("resource:type=%s 不支持", v2)
					}
				default:
				}
				r[k] = v2
			}
		}
		// 2. 检查服务
		var err error
		rt, _ := GetMapString(r, "type")
		switch rt {
		case cst.ResourceTypeEmpty:
			// pass 不做任何检查
		case cst.ResourceTypeMysql:
			err = ValidConnectMysql(r)
		case cst.ResourceTypeElasticsearch:
			err = ValidConnectES(r)
		case cst.ResourceTypeRabbitMQ:
			err = ValidConnectRabbitMQ(r)
		}
		if err != nil {
			bts, _ := json.Marshal(r)
			return nil, fmt.Errorf("资源连接错误:\nhost:%s\n错误信息:%s", string(bts), err.Error())
		}
		// 保存基本信息之外,保存连接信息等
		r["conn"] = nil
		idMap[_id] = r

	}
	return idMap, nil
}

// ValidConnectES 检查ES
func ValidConnectES(c map[string]interface{}) error {
	addr, ok := GetMapString(c, "dsn")
	if !ok || addr == "" {
		host, _ := GetMapString(c, "host")
		port, _ := GetMapString(c, "port")
		addr = host + ":" + port
	}
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	es7, err := elastic7.NewClient(
		elastic7.SetSniff(false),
		elastic7.SetURL(addr),
		elastic7.SetBasicAuth(user, pass),
	)
	defer func() {
		if es7 != nil {
			es7.Stop()
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

// ValidConnectRabbitMQ 检查RabbitMQ
func ValidConnectRabbitMQ(c map[string]interface{}) error {
	amqpURI, ok := GetMapString(c, "dsn")
	if !ok || amqpURI == "" {
		return fmt.Errorf("RabbitMQ连接只支持dsn格式:amqp://user:pwd@host:5672/vhost")
	}
	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName("datalink-consumer")
	conn, err := amqp.DialConfig(amqpURI, config)
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	conn.Close()
	return nil
}

// ValidConnectMysql 检查MySQL
func ValidConnectMysql(c map[string]interface{}) error {
	host, _ := GetMapString(c, "host")
	port, _ := GetMapString(c, "port")
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	addr := fmt.Sprintf("%s:%s", host, port)
	conn, err := client.Connect(addr, user, pass, "")
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		return err
	}
	err = conn.Ping()
	if err != nil {
		return err
	}
	return nil
}

// ValidSource 校验source参数
func ValidSource(c map[string]interface{}, conn map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
	arr, ok := GetMapArr(c, "source")
	if !ok {
		return nil, errors.New("need source item")
	}

	srcMap := map[string]map[string]interface{}{}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			// 检查不识别的参数
			for k1, _ := range v {
				switch k1 {
				case "resource_id", "sync_mode", "document_set", "extra":
				default:
					msg := fmt.Sprintf("source unsupport params:%s", k1)
					return nil, errors.New(msg)
				}
			}

			// 检查 resource_id
			resourceIdVal, _ := v["resource_id"]
			resourceId, _ := resourceIdVal.(string)
			if resourceId == "" {
				return nil, errors.New("source的resource_id不能为空")
			}

			resource, ok := conn[resourceId]
			if !ok {
				return nil, errors.New("source的resource_id不在资源集中")
			}

			// 检查 sync_mode
			syncMode, ok := GetMapString(v, "sync_mode")
			if !ok || !cst.InSyncMode(syncMode) {
				return nil, errors.New("source同步模式（sync_mode）不支持")
			}

			// syncMode=empty 时,不检查 document_set
			if syncMode != cst.ResourceTypeEmpty {
				// 检查 document_set,三种格式:
				// 1 "document_set":"document_set_name1"
				// 2.1 "document_set":{"document_set_name1":"document_set_name_alias"}
				// 2.2 "document_set":{"document_set_name1":["document_set_name_alias1","document_set_name_alias2"]}
				documentSetValueOrigin, _ := v["document_set"]

				var documentSetValue map[string]interface{}
				switch documentSetValueOrigin.(type) {
				case string:
					documentSetValue = map[string]interface{}{
						documentSetValueOrigin.(string): documentSetValueOrigin,
					}
				}
				if documentSetValue == nil {
					documentSetValue, _ = GetMapSI(v, "document_set")
				}
				for _, docSetAlias := range documentSetValue {
					switch aliasList := docSetAlias.(type) {
					case string:
						if aliasList == "" {
							return nil, errors.New("source文档集（document_set）需要设置别名")
						}
						list := strings.Split(aliasList, ",")
						for _, s := range list {
							srcMap[s] = v
						}
					case []interface{}:
						for _, alias := range aliasList {
							alias, _ := alias.(string)
							srcMap[alias] = v
						}
					default:
						return nil, errors.New("source 文档集格式为：字符串或者字符数组")
					}
				}
			}

			// 检查resource支持的类型
			var supportMode map[string]bool
			resourceType, _ := GetMapString(resource, "type")
			switch resourceType {
			case cst.ResourceTypeMysql:
				// 当source为MySQL,sync_mode=stream或replica时,检查开启bin_log,格式为row.
				switch syncMode {
				case cst.SyncModeStream, cst.SyncModeReplica:
					a := conn[resourceId]
					err := mysqlValidBinlog(a)
					if err != nil {
						return nil, err
					}
				}
				supportMode = map[string]bool{
					cst.SyncModeDirect:  true,
					cst.SyncModeStream:  true,
					cst.SyncModeEmpty:   true,
					cst.SyncModeReplica: true,
				}
				extra, ok := GetMapSI(v, "extra")
				if ok {
					for k2, _ := range extra {
						switch k2 {
						case "blHeader":
							blHeader, _ := extra["blHeader"]
							_, ok := blHeader.(bool)
							if !ok {
								return nil, errors.New("source额外参数（extra）blHeader值错误,必须为Boolean类型")
							}
						case "onDDL":
							ddlValue, _ := extra["onDDL"]
							_, ok := ddlValue.(bool)
							if !ok {
								return nil, errors.New("source额外参数（extra）onDDL值错误,必须为Boolean类型")
							}
						case "resume":
							_, ok := GetMapBool(extra, "resume")
							if !ok {
								return nil, errors.New("source额外参数（extra）resume值错误,必须为Boolean类型")
							}
						case "limit":
							_, ok := GetMapFloat64(extra, "limit")
							if !ok {
								return nil, errors.New("source额外参数（extra）limit value值错误,必须为Number类型")
							}
						default:
							return nil, fmt.Errorf("source 额外参数（extra）%s 不支持", k2)
						}
					}
				}
			case cst.ResourceTypeElasticsearch:
				supportMode = map[string]bool{
					cst.SyncModeDirect: true,
					cst.SyncModeEmpty:  true,
				}
			case cst.ResourceTypeRabbitMQ:
				supportMode = map[string]bool{
					cst.SyncModeStream: true,
					cst.SyncModeEmpty:  true,
				}
				extra, ok := GetMapSI(v, "extra")
				if !ok {
					return nil, fmt.Errorf("source 资源类型:%s 必须包含参数:queue_name,exchange,routing_key", resourceType)
				}

				must := []string{"queue_name", "exchange", "routing_key"}
				for _, f := range must {
					v, ok := extra[f]
					if !ok || v == "" {
						return nil, fmt.Errorf("source 资源类型:%s 必须包含参数:%s", resourceType, f)
					}
				}
			}
			_, ok = supportMode[syncMode]
			if !ok {
				return nil, fmt.Errorf("source 资源类型:%s 同步模式（sync_mode）:%s 不支持", resourceType, syncMode)
			}
		}
	}
	return srcMap, nil
}

// ValidTarget 校验target参数
func ValidTarget(c map[string]interface{}, conn map[string]map[string]interface{}) error {
	arr, ok := GetMapArr(c, "target")
	if !ok {
		return errors.New("need source item")
	}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			// 检查不识别的参数
			for k1, _ := range v {
				switch k1 {
				case "resource_id", "document_set", "extra":
				default:
					return fmt.Errorf("目标源（target）不支持参数:%s", k1)
				}
			}
			// 检查 resource_id
			resourceIdStr, _ := v["resource_id"]
			resourceId, _ := resourceIdStr.(string)
			resource, ok := conn[resourceId]
			if !ok {
				return errors.New("目标源（target）resource_id 不在文档集中")
			}

			// 检查 document_set
			documentSet, ok := GetMapSS(v, "document_set")
			if !ok || len(documentSet) == 0 {
				return errors.New("目标源（target）document_set can't empty")
			}

			// 检查 extra 数据
			// elasticsearch 有 limit 选项
			resourceType, _ := GetMapString(resource, "type")
			extra, _ := GetMapSI(v, "extra")
			switch resourceType {
			case cst.ResourceTypeEmpty:
				// pass 不做任何检查
			case cst.ResourceTypeMysql:
				// 不支持参数
				for k3, _ := range extra {
					return fmt.Errorf("目标源（target）type=%s, extra 不支持参数:%s", resourceType, k3)
				}
			case cst.ResourceTypeElasticsearch:
				for k3, v3 := range extra {
					switch k3 {
					case "limit":
						switch v3 := v3.(type) {
						case float64:
							if v3 < 1 {
								return errors.New("目标源（target）的extra 参数limit需要大于1")
							}
						default:
							return errors.New("目标源（target）的extra 参数limit的值错误")
						}
					default:
						return fmt.Errorf("目标源（target）的extra 参数%s 不支持", k3)
					}
				}
			}
		}
	}
	return nil
}

// ValidPipeline pipeline的校验
func ValidPipeline(c map[string]interface{}, smap map[string]map[string]interface{}) error {
	arr, ok := GetMapArr(c, "pipeline")
	if !ok {
		return errors.New("需要设置pipeline部分")
	}
	for _, v := range arr {
		switch v := v.(type) {
		case map[string]interface{}:
			documentSetValue, ok := GetMapString(v, "document_set")
			documentSet := strings.Split(documentSetValue, ",")
			if len(documentSet) < 1 {
				return errors.New("pipeline的文档集（document_set）不能为空")
			}
			for _, docSet := range documentSet { // 检查source中document_set是否有效
				_, ok = smap[docSet]
				if !ok {
					return fmt.Errorf("pipeline的文档集（document_set）:%s 在资源中不存在", docSet)
				}
			}

			flow, _ := GetMapArr(v, "flow")
			if len(flow) == 0 {
				return fmt.Errorf("pipeline的文档集（document_set）%s, 处理流（flow）不能为空", documentSetValue)
			}
			err := ValidPipelineFlow(flow, smap)
			if err != nil {
				return fmt.Errorf("pipeline的文档集（document_set）%s校验错误, 错误信息:%s", documentSetValue, err.Error())
			}
		}
	}
	return nil
}

// ValidPipelineFlow 校验处理流（flow）中的处理流单元
func ValidPipelineFlow(flow []interface{}, smap map[string]map[string]interface{}) error {
	for i, v := range flow {
		switch v := v.(type) {
		case map[string]interface{}:
			t, _ := GetMapString(v, "type")
			switch t {
			case "map", "filter":
				script, _ := GetMapString(v, "script")
				if script == "" {
					return fmt.Errorf("处理流（flow）位置:%d, 脚本不能为空", i)
				}
				_, err := loop.NewScript(script, nil)
				if err != nil {
					return fmt.Errorf("处理流（flow）位置:%d, 脚本错误:%s", i, err.Error())
				}
			case "relateInline":
				// 关联类型
				assocType, _ := GetMapString(v, "assoc_type")
				if assocType == "" || (assocType != "11" && assocType != "1n") {
					return errors.New("处理流单元（relateInline）中，参数assoc_type（关联类型）值只能是一对一（11）或者一堆多（1n）")
				}
				// 关联文档
				relateDocumentSet, _ := GetMapString(v, "relate_document_set")
				if relateDocumentSet == "" {
					return errors.New("处理流单元（relateInline）中，参数relate_document_set(关联文档集)不能为空")
				}
				// 关联层级:sub子集,同级sib
				layerType, _ := GetMapString(v, "layer_type")
				if layerType != "sub" && layerType != "sib" {
					return errors.New("处理流单元（relateInline）中，参数layer_type值只能是sub或者sib")
				}
				// 当为子集时,key名称 sub_label
				if layerType == "sub" {
					subLabel, _ := GetMapString(v, "sub_label")
					if subLabel == "" {
						return errors.New("处理流单元（relateInline）中，当参数layer_type=sub是sub_label不能为空")
					}
				}
				// 当为同级时,field_map 字段映射,其他...
				if layerType == "sib" {
					_, ok := GetMapSI(v, "field_map")
					if !ok {
						return errors.New("处理流单元（relateInline）中，当参数layer_type=sib时field_map必须指定字段映射")
					}
				}
				// where 关联关系,条件不能为空
				wheres, _ := GetMapArr(v, "wheres")
				if len(wheres) == 0 {
					return errors.New("处理流单元（relateInline）中，参数条件列表（wheres）不能为空")
				}
			case "mapInline":
				fieldMap, ok := GetMapArr(v, "field_map")
				if !ok || len(fieldMap) < 1 {
					return errors.New("处理流单元（mapInline）中，参数field_map（字段映射）必须设置值")
				}
				for _, fieldItem := range fieldMap {
					fieldItem, _ := fieldItem.(map[string]interface{})
					if len(fieldItem) < 4 {
						return errors.New("处理流单元（mapInline）中，参数field_map格式错误")
					}
					temp, _ := GetMapString(fieldItem, "srcField")
					if temp == "" {
						return errors.New("处理流单元（mapInline）中，参数srcField不能为空")
					}
					temp, _ = GetMapString(fieldItem, "srcType")
					if temp == "" {
						return errors.New("处理流单元（mapInline）中，参数srcType不能为空")
					}
					temp, _ = GetMapString(fieldItem, "aimField")
					if temp == "" {
						return errors.New("处理流单元（mapInline）中，参数aimField不能为空")
					}
					temp, _ = GetMapString(fieldItem, "aimType")
					if temp == "" {
						return errors.New("处理流单元（mapInline）中，参数aimType不能为空")
					}
				}
			default:
				return fmt.Errorf("管道处理（pipeline）中,处理流(flow)内的处理单元不支持：%s ", t)
			}
		}
	}
	return nil
}

// GetMapFloat64 float64
func GetMapFloat64(c map[string]interface{}, n string) (float64, bool) {
	v, ok := c[n]
	if !ok {
		return 0, false
	}
	switch v := v.(type) {
	case float64:
		return v, true
	}
	return 0, false
}

// GetMapBool bool
func GetMapBool(c map[string]interface{}, n string) (bool, bool) {
	v, ok := c[n]
	if !ok {
		return false, false
	}
	switch v := v.(type) {
	case bool:
		return v, true
	}
	return false, false
}

// GetMapString string
func GetMapString(c map[string]interface{}, n string) (string, bool) {
	v, ok := c[n]
	if !ok {
		return "", false
	}
	switch v := v.(type) {
	case string:
		return v, true
	}
	return "", false
}

// GetMapSS map[string]string
func GetMapSS(c map[string]interface{}, n string) (map[string]string, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v1 := v.(type) {
	case map[string]interface{}:
		v3 := map[string]string{}
		for k2, v2 := range v1 {
			switch v2 := v2.(type) {
			case string:
				v3[k2] = v2
			default:
			}
		}
		return v3, true
	case map[string]string:
		return v1, true
	}
	return nil, false
}

// GetMapSI map[string]interface{}
func GetMapSI(c map[string]interface{}, n string) (map[string]interface{}, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v := v.(type) {
	case map[string]interface{}:
		return v, true
	}
	return nil, false
}

// GetMapArr 获取[]interface{}
func GetMapArr(c map[string]interface{}, n string) ([]interface{}, bool) {
	v, ok := c[n]
	if !ok {
		return nil, false
	}
	switch v := v.(type) {
	case []interface{}:
		return v, true
	}
	return nil, false
}

// mysqlValidBinlog 检查binlog格式
func mysqlValidBinlog(c map[string]interface{}) error {
	host, _ := GetMapString(c, "host")
	port, _ := GetMapString(c, "port")
	user, _ := GetMapString(c, "user")
	pass, _ := GetMapString(c, "pass")
	addr := fmt.Sprintf("%s:%s", host, port)

	conn, err := client.Connect(addr, user, pass, "")
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	if err != nil {
		return err
	}
	ret, err := conn.Execute("SHOW VARIABLES LIKE 'log_bin'")
	if err != nil {
		return err
	}
	arr := terms.MySQLReadResultToSlice(ret)
	if len(arr) != 1 {
		return errors.New("log_bin 检查错误")
	}
	if arr[0]["Value"] != "ON" {
		return errors.New("log_bin 需要开启")
	}
	return nil
}
