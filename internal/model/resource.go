package model

import (
	"context"
	"data-link-2.0/datalink/terms"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	elastic7 "github.com/olivere/elastic/v7"
)

const (
	DataBaseTypeMysql         = "mysql"
	DataBaseTypeElasticsearch = "elasticsearch"
)

// Resource 资源,数据源的连接参数
type Resource struct {
	Id    string            // 自增主键
	Desc  string            // 连接描述
	Type  string            // resource type 数据类型
	User  string            // 账户
	Pass  string            // 密码
	Host  string            // 服务器
	Port  string            // 端口
	Extra map[string]string // rabbit 多一个概念vhost概念,放入
	Dsn   string            // schema/不同数据源schema不同,优先使用改值
}

// Ping 检查是否连通
func (m *Resource) Ping() (bool, error) {
	switch m.Type {
	case DataBaseTypeMysql:
		DB, err := m.ConnMysql()
		if err != nil {
			return false, err
		}
		defer DB.Close()
		err = DB.Ping()
		if err != nil {
			return false, err
		}
	case DataBaseTypeElasticsearch:
		es7, err := m.ConnElasticSearch()
		if err != nil {
			return false, err
		}
		defer es7.Stop()
	default:
		return false, errors.New("not support")
	}
	return true, nil
}

// ConnMysql 连接对象
func (m *Resource) ConnMysql() (*client.Conn, error) {
	addr := fmt.Sprintf("%s:%s", m.Host, m.Port)
	return client.Connect(addr, m.User, m.Pass, "")
}

// ConnElasticSearch es连接
func (m *Resource) ConnElasticSearch() (*elastic7.Client, error) {
	opts := []elastic7.ClientOptionFunc{
		elastic7.SetSniff(false),
		elastic7.SetHealthcheck(false),
	}
	addr := m.Dsn
	if addr != "" {
		opts = append(opts, elastic7.SetURL(addr))
	} else {
		addr = m.Host + ":" + m.Port
		opts = append(opts, elastic7.SetURL(addr))
		opts = append(opts, elastic7.SetBasicAuth(m.User, m.Pass))
	}
	es7, err := elastic7.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	_, code, err := es7.Ping(addr).Do(context.Background())
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, errors.New(fmt.Sprintf("elasticsearch error,http code:%d", code))
	}
	return es7, nil
}

// DocumentSets 获取资源集列表
// 目前支持mysql的表列表
func (m *Resource) DocumentSets() ([]map[string]interface{}, error) {
	switch m.Type {
	case DataBaseTypeMysql:
		return m.DocumentSetMysql()
	case DataBaseTypeElasticsearch:
		return m.DocumentSetElasticsearch()
	default:
		return nil, errors.New("不支持的数据类型")
	}
}

// DocumentSetMysql 查找所有数据集
func (m *Resource) DocumentSetMysql() ([]map[string]interface{}, error) {
	DB, err := m.ConnMysql()
	if err != nil {
		return nil, err
	}
	defer DB.Close()

	// show database
	result, err := DB.Execute("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	rets := terms.MySQLReadResultToSlice(result)
	if err != nil {
		return nil, err
	}
	var databases []map[string]interface{}
	for _, database := range rets {
		bts := database["Database"].([]byte)
		dbname := string(bts)
		switch dbname {
		case "information_schema", "performance_schema", "mysql":
			continue
		}
		// change db
		_, err := DB.Execute(fmt.Sprintf("USE `%s`", dbname))
		if err != nil {
			return nil, err
		}

		// show tables
		result, err := DB.Execute("SHOW TABLES")
		if err != nil {
			return nil, err
		}
		tabs := terms.MySQLReadResultToSlice(result)

		// desc tables
		var tables []map[string]interface{}
		for _, tab := range tabs {
			var tableName string
			for _, v := range tab {
				tableName = string(v.([]uint8))
				break
			}
			result, err := DB.Execute(fmt.Sprintf("DESC `%s`", tableName))
			if err != nil {
				return nil, err
			}
			rets := terms.MySQLReadResultToSlice(result)
			cols := map[string]interface{}{}
			for _, col := range rets {
				Field := col["Field"].([]uint8)
				Type := col["Type"].([]uint8)
				Key := col["Key"].([]uint8)
				field := string(Field)
				cols[field] = map[string]interface{}{
					"field": field,
					"type":  string(Type),
					"key":   string(Key),
				}
			}
			tables = append(tables, map[string]interface{}{
				"layer": "second",
				"name":  tableName,
				"alias": "table",
				"cols":  cols,
			})
		}
		base := map[string]interface{}{
			"layer": "top",
			"name":  dbname,
			"alias": "database",
			"sub":   tables,
		}
		databases = append(databases, base)
	}
	return databases, nil
}

// DocumentSetElasticsearch es的索引列表
func (m *Resource) DocumentSetElasticsearch() ([]map[string]interface{}, error) {
	client, err := m.ConnElasticSearch()
	if err != nil {
		return nil, err
	}
	defer client.Stop()
	indexNames, err := client.IndexNames()
	if err != nil {
		return nil, err
	}
	var database []map[string]interface{}
	for _, indexName := range indexNames {
		database = append(database, map[string]interface{}{
			"layer": "top",
			"name":  indexName,
			"alias": "index",
			"sub":   map[string]interface{}{},
		})
	}
	return database, nil
}

// ToMap 转为map
func (m *Resource) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"id":    m.Id,
		"desc":  m.Desc,
		"type":  m.Type,
		"user":  m.User,
		"pass":  m.Pass,
		"host":  m.Host,
		"port":  m.Port,
		"extra": m.Extra,
		"dsn":   m.Dsn,
	}
}

// Load 填充数据
func (m *Resource) Load(d map[string]interface{}) {
	if v, ok := d["id"]; ok {
		if v, ok := v.(string); ok {
			m.Id = v
		}
	}
	if v, ok := d["desc"]; ok {
		m.Desc = v.(string) // 连接描述
	}
	if v, ok := d["type"]; ok {
		m.Type = v.(string) // resource type 数据类型
	}
	if v, ok := d["user"]; ok {
		m.User = v.(string) // 账户
	}
	if v, ok := d["pass"]; ok {
		m.Pass = v.(string) // 密码
	}
	if v, ok := d["host"]; ok {
		m.Host = v.(string) // 服务器
	}
	if v, ok := d["port"]; ok {
		m.Port = v.(string) // 端口
	}
	// 其他参数
	// rabbit 多一个概念vhost概念,放入
	if v, ok := d["extra"]; ok && v != nil {
		m.Extra = v.(map[string]string)
	}
	// dsn
	if v, ok := d["dsn"]; ok && v != nil {
		m.Dsn = v.(string)
	}
}
