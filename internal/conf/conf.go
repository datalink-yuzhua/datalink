package conf

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type LinkConf struct {
	AppDebug            bool
	PS                  string // os.PathSeparator
	CoreNumber          int    `toml:"core-number"`
	PipelineWorkerCount int    `toml:"pipeline-worker-count"`
	LinkDHost           string `toml:"linkd-host"`
	LinkDPort           string `toml:"linkd-port"`
	LinkDAuth           string `toml:"linkd-auth"`
	LogPath             string // 日志文件目录
	TaskResumePath      string // 任务继续文件目录
	TaskPath            string // 任务文件目录
	TaskErrorPath       string // 任务错误目录
	BinDir              string // bin文件目录
	BaseDir             string // 项目目录
	WebDir              string // web目录
}

// GetBasicAuth 获取账户
func (_this LinkConf) GetBasicAuth() (user string, pass string, ok bool) {

	authStr := _this.LinkDAuth
	auth := strings.Split(authStr, ":")
	if len(auth) != 2 {
		return "", "", false
	}

	return auth[0], auth[1], true
}

// G 全局配置单例
var G *LinkConf

// LoadConf 加载配置文件
func LoadConf(file string) (conf *LinkConf) {

	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Panicln(err.Error())
		}
	}(f)
	if err != nil {
		log.Panicln(err.Error())
	}
	// 解析数据
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Panicln(err.Error())
	}
	str := string(bytes)
	if _, err := toml.Decode(str, &conf); err != nil {
		log.Panicln(err.Error())
	}

	ps := string(os.PathSeparator)
	conf.PS = ps

	// 获取当前二进制文件目录
	binPath, err := os.Executable()
	if err != nil {
		log.Panicln(err.Error())
	}
	conf.BinDir = filepath.Dir(binPath)
	conf.BaseDir = filepath.Dir(filepath.Dir(conf.BinDir))
	conf.WebDir = fmt.Sprintf("%s%slinkadmin%s", conf.BaseDir, ps, ps)

	conf.LogPath = fmt.Sprintf("%s%sruntime%slog%s", conf.BaseDir, ps, ps, ps)
	conf.TaskResumePath = fmt.Sprintf("%s%sruntime%sresume%s", conf.BaseDir, ps, ps, ps)
	conf.TaskPath = fmt.Sprintf("%s%sruntime%stask%s", conf.BaseDir, ps, ps, ps)
	conf.TaskErrorPath = fmt.Sprintf("%s%sruntime%serror%s", conf.BaseDir, ps, ps, ps)

	G = conf
	return conf
}

type Resource struct {
	Id   string `json:"id"`
	Desc string `json:"desc"`
	Type string `json:"type"`
	User string `json:"user"`
	Pass string `json:"pass"`
	Host string `json:"host"`
	Port string `json:"port"`
	Dsn  string `json:"dsn"`
}

// ReaderDocumentSet 对单个数据集的设置项
type ReaderDocumentSet struct {
	Name  string   // 集合名称
	Alias []string // 别名
}
type Reader struct {
	ResourceId     string      `json:"resource_id"`
	SyncMode       string      `json:"sync_mode"`
	DocumentSetOri interface{} `json:"document_set"`
	DocumentSet    map[string]ReaderDocumentSet
	Extra          map[string]interface{} `json:"extra"`
}

type Writer struct {
	ResourceId  string                 `json:"resource_id"`
	DocumentSet map[string]string      `json:"document_set"`
	Extra       map[string]interface{} `json:"extra"`
}

type Map struct {
	DocumentSet string `json:"document_set"`
	Script      string `json:"script"`
}

type Filter struct {
	DocumentSet string `json:"document_set"`
	Script      string `json:"script"`
}

type TaskConf struct {
	Setup     map[string]interface{}   `json:"setup"`    // task 设置
	Resources []Resource               `json:"resource"` // 资源配置
	Sources   []Reader                 `json:"source"`   // 来源部分
	Targets   []Writer                 `json:"target"`   // 写入部分
	Pipelines []map[string]interface{} `json:"pipeline"` // pipeline
	Space     []map[string]interface{} `json:"space"`    // buffer,
}

// New 初始化Task配置
func New(bts []byte) (TaskConf, error) {
	var tc TaskConf
	err := json.Unmarshal(bts, &tc)
	if err != nil {
		return tc, err
	}

	// 处理reader中的documentSet的别名
	var sources []Reader
	for _, reader := range tc.Sources {
		ds := map[string]ReaderDocumentSet{}
		switch r := reader.DocumentSetOri.(type) {
		case string:
			a := strings.Split(r, ",")
			for _, docSet := range a {
				rd := ReaderDocumentSet{}
				rd.Name = docSet
				rd.Alias = []string{rd.Name}
				ds[docSet] = rd
			}
		case map[string]interface{}:
			for docSet, aliasList := range r {
				rd := ReaderDocumentSet{}
				rd.Name = docSet
				switch aliasList := aliasList.(type) {
				case string:
					rd.Alias = []string{aliasList}
				case []interface{}:
					for _, aliasVal := range aliasList {
						alias, _ := aliasVal.(string)
						rd.Alias = append(rd.Alias, alias)
					}
				}
				ds[docSet] = rd
			}
		default:
			return tc, errors.New("unsupported source format")
		}
		reader.DocumentSet = ds
		sources = append(sources, reader)
	}
	tc.Sources = sources

	return tc, nil
}
