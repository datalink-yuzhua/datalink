package terms

import (
	"data-link-2.0/internal/conf"
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	elastic7 "github.com/olivere/elastic/v7"
	"strings"
)

// ElasticSearchConn 连接
func ElasticSearchConn(rc conf.Resource) (*elastic7.Client, error) {
	addr := rc.Dsn
	if addr == "" {
		addr = fmt.Sprintf("%s:%s", rc.Host, rc.Port)
	}
	if !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return elastic7.NewClient(
		elastic7.SetSniff(false),
		elastic7.SetURL(addr),
		elastic7.SetBasicAuth(rc.User, rc.Pass),
	)
}

// MySQLConn 连接
func MySQLConn(rc conf.Resource) (*client.Conn, error) {
	addr := fmt.Sprintf("%s:%s", rc.Host, rc.Port)
	return client.Connect(addr, rc.User, rc.Pass, "")
}

// MySQLReadResultToSlice 读取数据结果
func MySQLReadResultToSlice(result *mysql.Result) (arr []map[string]interface{}) {
	rowNumber := result.RowNumber()

	for i := 0; i < rowNumber; i++ {
		doc := map[string]interface{}{}
		for fieldName, j := range result.FieldNames {
			d, _ := result.GetValue(i, j)
			switch d.(type) {
			case nil:
				doc[fieldName] = nil
			default:
				doc[fieldName], _ = result.GetString(i, j)
			}
		}
		arr = append(arr, doc)
	}
	return arr
}

// MySQLTableStructField 获取表字段,以及主键
func MySQLTableStructField(cc *client.Conn, db string, tab string) (map[string]bool, string, error) {
	err := cc.UseDB(db)
	if err != nil {
		return nil, "", err
	}
	result, err := cc.Execute(fmt.Sprintf("DESC `%s`", tab))
	if err != nil {
		return nil, "", err
	}

	var pkName string
	ts := map[string]bool{}
	rows := MySQLReadResultToSlice(result)
	for _, row := range rows {
		fieldValue, _ := row["Field"]
		keyValue, _ := row["Key"]
		fieldName, _ := fieldValue.(string)
		fieldKey, _ := keyValue.(string)
		ts[fieldName] = false
		if fieldKey == "PRI" {
			pkName = fieldName
			ts[fieldName] = true
		}
	}
	return ts, pkName, nil
}
