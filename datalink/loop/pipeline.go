package loop

import (
	"data-link-2.0/datalink/terms"
	"data-link-2.0/internal/helper"
	"data-link-2.0/internal/jsvm"
	"data-link-2.0/internal/log"
	"data-link-2.0/internal/msg"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Flow  otto虚拟机的执行单元
type Flow struct {
	VM      *otto.Otto    // otto
	task    *Task         // task
	TimeOut time.Duration // 超时执行单位秒
	Script  string        // js脚本
	FunType uint8         // 函数类型:1map,2filter,3relateInline,4mapInline
	Lock    sync.RWMutex  // 需要锁
}

// RelateInlineFlow 关联数据
type RelateInlineFlow struct {
	flow        *Flow                                                 // 使用脚本操作
	docSetAlias string                                                // 文档集,此处应该使用的是别名
	relateImp   terms.ReaderRunImp                                    // 关联数据源,用于读取数据(不填写默认关联读取源)
	handle      func(op *msg.Op) (arr []*msg.Op)                      // 使用内部定义的关联关系
	whereHandle func(op *msg.Op) (wheres []map[string][2]interface{}) // 格式化参数
	itemConf    map[string]interface{}                                // flow中的单个配置
}

// MapInlineFlow 映射字段
type MapInlineFlow struct {
	flow     *Flow                    // 使用脚本操作
	fieldMap map[string]interface{}   // 字段映射
	handle   func(op *msg.Op) *msg.Op // 转换文档
}

// Pipeline 用于管道处理
type Pipeline struct {
	TaskId string                   // 定位task
	conf   []map[string]interface{} // pipeline 配置
	// 流程顺序
	// [{"table_name":[{"map":"3"},{"filter":"4"}]}]
	flowMap       map[string][]map[string]string // 执行顺序
	maps          map[string]*Flow               // map {"1":flow, "3":flow}
	filters       map[string]*Flow               // filter {"2":flow}
	relateInlines map[string]*RelateInlineFlow   // relateInline
	mapInlines    map[string]*MapInlineFlow      // mapInline
}

// NewPipeline 构建pipeline
func NewPipeline(taskId string, config []map[string]interface{}) *Pipeline {
	p := new(Pipeline)
	p.conf = config
	p.flowMap = map[string][]map[string]string{}
	p.maps = map[string]*Flow{}
	p.filters = map[string]*Flow{}
	p.relateInlines = map[string]*RelateInlineFlow{}
	p.mapInlines = map[string]*MapInlineFlow{}
	p.TaskId = taskId
	return p
}

// 使用包的格式处理pipeline
var d = make(map[string]*Pipeline)

// RegisterFlow 注册able
func RegisterFlow(p *Pipeline, t *Task) (ok bool, err error) {
	locker := sync.RWMutex{}
	locker.Lock()
	defer locker.Unlock()

	// 一维 应该是taskId
	// 二维 应该是documentSet
	// 三维 应该是sort

	var i int
	for _, pipelineItem := range p.conf {
		docSetAlias, _ := pipelineItem["document_set"].(string)
		if docSetAlias == "" {
			return false, errors.New("管道处理（pipeline）需要指定文档集（document_set）")
		}

		flows, ok := pipelineItem["flow"]
		if !ok {
			return false, errors.New("管道处理（pipeline）需要指定文档集（document_set）")
		}

		flowValue := make([]map[string]string, 0)

		for _, flowItemV := range flows.([]interface{}) {
			flowItem := flowItemV.(map[string]interface{})
			itemType, _ := flowItem["type"]
			pipeType := itemType.(string)
			if pipeType == "" {
				return false, errors.New("管道处理（pipeline）中的处理流（flow）需要指定处理流单元类型")
			}

			idx := strconv.Itoa(i)
			switch pipeType {
			case "map": // js字段映射
				val, _ := flowItem["script"]
				script, _ := val.(string)
				if script == "" {
					return false, errors.New("管道处理（pipeline）中的处理流（flow）,当处理流单元（item）类型为map时脚本不能为空")
				}

				a, err := NewScript(script, t)
				if err != nil {
					return false, errors.New(err.Error())
				}
				a.FunType = 1
				p.maps[idx] = a
			case "filter": // js的过滤
				val, _ := flowItem["script"]
				script, _ := val.(string)
				if val == "" {
					return false, errors.New("管道处理（pipeline）中的处理流（flow）,当处理流单元（item）类型为filter时脚本不能为空")
				}

				a, err := NewScript(script, t)
				if err != nil {
					return false, errors.New(err.Error())
				}
				a.FunType = 2
				p.filters[idx] = a
			case "relateInline": // inline的关联
				// 一部分数据只能运行的时候注册
				a, err := NewRelateInline(flowItem, docSetAlias)
				if err != nil {
					return false, err
				}
				p.relateInlines[idx] = a
			case "mapInline": // inline的字段映射
				a, err := NewMapInline(flowItem, docSetAlias)
				if err != nil {
					return false, err
				}
				p.mapInlines[idx] = a
			default:
				return false, errors.New("管道处理（pipeline）中的处理流（flow）不支持的处理流单元类型")
			}

			flowValue = append(flowValue, map[string]string{pipeType: strconv.Itoa(i)})
			i++
		}
		p.flowMap[docSetAlias] = flowValue
	}

	d[p.TaskId] = p
	return true, nil
}

// RemoveFlow 移除able
func RemoveFlow(taskId string) {
	locker := sync.RWMutex{}
	locker.Lock()
	defer locker.Unlock()
	delete(d, taskId)
}

// NewScript 构建map和filter
func NewScript(script string, t *Task) (a *Flow, err error) {
	if script == "" {
		return nil, errors.New("脚本不能为空")
	}
	vm := otto.New()
	if err := vm.Set("module", make(map[string]interface{})); err != nil {
		return nil, err
	}
	if _, err := vm.Run(script); err != nil {
		return nil, err
	}
	val, err := vm.Run("module.exports")
	if err != nil {
		return nil, err
	} else if !val.IsFunction() {
		return nil, errors.New("module.exports 必须是一个函数")
	}

	a = &Flow{VM: vm, Script: script}
	err = a.buildIn(t)
	if err != nil {
		return nil, err
	}
	err = a.buildOut()
	if err != nil {
		return nil, err
	}
	return a, nil
}

// NewMapInline 构建mapInline
func NewMapInline(config map[string]interface{}, docSet string) (*MapInlineFlow, error) {
	m := new(MapInlineFlow)
	fmv, ok := config["field_map"]
	if !ok {
		return nil, errors.New("field_map 格式错误")
	}

	fm1 := map[string]interface{}{}
	for _, v := range fmv.([]interface{}) {
		v2, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		srcFiled, _ := v2["srcField"].(string)
		if srcFiled == "" {
			return nil, errors.New("field_map 格式错误")
		}
		fm1[srcFiled] = v
	}
	m.fieldMap = fm1
	return m, nil
}

// Handle 处理op消息
func (m *MapInlineFlow) Handle(opInput *msg.Op) (map[string]interface{}, error) {
	inDoc := opInput.Doc
	if len(inDoc) < 1 {
		return inDoc, nil
	}
	for field, srcValue := range inDoc {
		mapRule, ok := m.fieldMap[field]
		if !ok {
			continue
		}
		//{
		//	"srcField": "id",
		//	"srcType": "long",
		//	"aimField": "tab_id",
		//	"aimType": "string"
		//}
		fieldRule := mapRule.(map[string]interface{})
		//src
		srcFieldValue, _ := fieldRule["srcField"]
		srcField, _ := srcFieldValue.(string)
		srcTypeValue, _ := fieldRule["srcType"]
		srcType, _ := srcTypeValue.(string)
		if srcField == "" || srcType == "" {
			continue
		}
		switch srcType {
		case "long":
			srcValue = m.MixedToLong(srcValue)
		case "float":
			srcValue = m.MixedToFloat(srcValue)
		case "string":
			srcValue = m.MixedToString(srcValue)
		default:
			return inDoc, errors.New("srcType不支持的类型:" + srcType)
		}

		// aim
		aimFieldValue, _ := fieldRule["aimField"]
		aimField, _ := aimFieldValue.(string)
		aimTypeValue, _ := fieldRule["aimType"]
		aimType, _ := aimTypeValue.(string)
		if aimField == "" || aimType == "" {
			continue
		}
		switch aimType {
		case "long":
			inDoc[aimField] = m.MixedToLong(srcValue)
		case "float":
			inDoc[aimField] = m.MixedToFloat(srcValue)
		case "string":
			inDoc[aimField] = m.MixedToString(srcValue)
		default:
			return inDoc, errors.New("aimType不支持的类型:" + aimType)
		}
		// 名字不同的时候才删除旧值
		if aimField != srcField {
			delete(inDoc, field)
		}
	}
	return inDoc, nil
}

// MixedToLong 数据转为int
func (m *MapInlineFlow) MixedToLong(v interface{}) int64 {
	switch v := v.(type) {
	case []byte:
		val, _ := strconv.ParseInt(string(v), 10, 64)
		return val
	case string:
		val, _ := strconv.ParseInt(v, 10, 64)
		return val
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// MixedToFloat 数据转为Float
func (m *MapInlineFlow) MixedToFloat(v interface{}) float64 {
	switch v := v.(type) {
	case []byte:
		val, _ := strconv.ParseFloat(string(v), 64)
		return val
	case string:
		val, _ := strconv.ParseFloat(v, 10)
		return val
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	default:
		return 0.0
	}
}

// MixedToString 数据转为字符串
func (m *MapInlineFlow) MixedToString(v interface{}) string {
	switch v := v.(type) {
	case []uint8:
		return string(v)
	case uint64:
		return strconv.FormatUint(v, 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64, float32:
		return fmt.Sprintf("%f", v)
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// NewRelateInline 构建relateInline
func NewRelateInline(config map[string]interface{}, docSet string) (*RelateInlineFlow, error) {
	m := new(RelateInlineFlow)
	m.itemConf = config
	m.docSetAlias = docSet
	err := m.Init()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Init 初始化
func (m *RelateInlineFlow) Init() (err error) {
	// 1.使用脚本初始化
	script, _ := m.itemConf["script"]
	if script != "" {
		return errors.New("暂不支持 RelateInlineFlow")
	}

	// 2.使用内置规则初始化
	wheres, _ := m.itemConf["wheres"]
	wheresArr, _ := wheres.([]interface{})
	if len(wheresArr) < 1 {
		return errors.New("必须设置wheres")
	}

	// 字段映射
	var whereArr [][4]interface{}
	for _, whereArrItem := range wheresArr {
		whereMap := whereArrItem.(map[string]interface{})
		srcField, _ := whereMap["src_field"]
		srcFieldStr, _ := srcField.(string)
		relField, _ := whereMap["rel_field"]
		relFieldStr, _ := relField.(string)
		if relFieldStr == "" {
			log.Error("rel_field需要设置字符名称")
			continue
		}
		operator, _ := whereMap["operator"]
		operatorStr, _ := operator.(string)
		if operatorStr == "" {
			log.Error("relate需要关系符号（operator）")
			continue
		}
		constValue, _ := whereMap["const_value"]
		if constValue == nil && srcFieldStr == "" {
			log.Error("const_value 需要设置值")
			continue
		}
		whereArr = append(whereArr, [...]interface{}{srcFieldStr, operatorStr, relFieldStr, constValue})
	}
	whereArrPointer := &whereArr

	// 组合查询参数
	m.whereHandle = func(op *msg.Op) (wheres []map[string][2]interface{}) {
		// 拼接值
		var wheresFun []map[string][2]interface{}
		for _, whereItem := range *whereArrPointer {
			srcF := whereItem[0]   // 源文档字段
			opF := whereItem[1]    // 逻辑符号
			relF := whereItem[2]   // 关联字段
			constF := whereItem[3] // 常量值,需要转成字符串

			constFS := ""
			switch constF := constF.(type) {
			case nil:
			case []uint8:
				constFS = string(constF)
			case string:
				constFS = constF
			case int64, uint64, int32, uint32, int16, uint16:
				constFS = fmt.Sprintf("%d", constF)
			case float32:
				// 没有小数点,转为整数
				if float32(int64(constF)) == constF {
					constFS = fmt.Sprintf("%d", int64(constF))
				} else {
					constFS = fmt.Sprintf("%f", constF)
				}
			case float64:
				// 没有小数点,转为整数
				if float64(int64(constF)) == constF {
					constFS = fmt.Sprintf("%d", int64(constF))
				} else {
					constFS = fmt.Sprintf("%f", constF)
				}
			default:
				constFS = fmt.Sprint(constF)
			}

			srcFS, _ := srcF.(string)
			srcValue, ok1 := op.Doc[srcFS]
			if ok1 == false {
				if constFS == "" {
					log.Info(fmt.Sprintf("一对一的关联查询字段:%s 不存在", srcFS))
					return wheresFun
				}
				srcValue = constFS
			}

			operator := opF
			relField, _ := relF.(string)
			wheresFun = append(wheresFun, map[string][2]interface{}{relField: {operator, srcValue}})
		}
		return wheresFun
	}
	return nil
}

// SetHandle 设置handle
func (m *RelateInlineFlow) SetHandle(t *Task) (err error) {
	assocType, ok := m.itemConf["assoc_type"]
	if !ok {
		return errors.New("关联类型（assoc_type）需要值")
	}

	// 转换为 op 需要注意 sib(同级) 或则 sub(子集)
	layerType, _ := m.itemConf["layer_type"]
	layerTypeStr, _ := layerType.(string)
	if layerTypeStr == "" {
		layerTypeStr = "sub"
	}

	// 同级时需要处理字段冲突问题
	var layerTypeSubName string
	var fieldMapFun func(s map[string]interface{}, r map[string]interface{}) (m map[string]interface{})
	fieldMapOrigin, _ := m.itemConf["field_map"]
	fieldMap, _ := fieldMapOrigin.(map[string]interface{})
	if layerTypeStr == "sib" && len(fieldMap) > 0 {
		//"field_map": {
		//	"t.id": "uid",
		//	"s.name": "uname"
		//}
		// 需要处理字段覆盖的问题
		// 对源和目标的转换
		fieldMapSrc := map[string][]string{}
		fieldMapRelate := map[string][]string{}
		for originField, valueField := range fieldMap {
			fa := strings.Split(originField, ".")
			vf, _ := valueField.(string)
			var vfArr []string
			vfExpArr := strings.Split(vf, "|")
			vfArr = append(vfArr, vfExpArr[0])
			if len(vfExpArr) == 2 {
				vfArr = append(vfArr, strings.Split(vfExpArr[1], ":")...)
				if len(vfArr) != 3 {
					return errors.New("关联查询参数错误:" + vf)
				}
			}
			l := len(fa)
			if l == 1 {
				fieldMapSrc[fa[0]] = vfArr
			}
			// field mapping error
			if l != 2 {
				continue
			}
			if fa[0] == "r" {
				fieldMapRelate[fa[1]] = vfArr
			} else {
				fieldMapSrc[fa[1]] = vfArr
			}
		}
		validEach := func(s map[string]interface{}, fm map[string][]string) map[string]interface{} {

			// 处理有效字段
			doc := map[string]interface{}{}
			for srcField, srcValue := range s {
				var newFieldName string
				newFieldExp, _ := fm[srcField]
				if len(newFieldExp) == 0 {
					continue
				}
				newFieldName = newFieldExp[0]

				// 处理default值
				if srcValue == nil && len(newFieldExp) == 3 {
					srcValue = fieldMapDefaultValue(newFieldExp)
				}
				doc[newFieldName] = srcValue
			}

			// 处理null值
			for _, newFieldExp := range fm {
				vField := newFieldExp[0]
				v, _ := doc[vField]
				if v == nil && len(newFieldExp) == 3 {
					doc[vField] = fieldMapDefaultValue(newFieldExp)
				}
			}
			return doc
		}
		// s为主文档,r为关联文档
		fieldMapFun = func(s map[string]interface{}, r map[string]interface{}) (m map[string]interface{}) {

			s = validEach(s, fieldMapSrc)
			r = validEach(r, fieldMapRelate)

			return helper.MapsMerge(s, r)
		}
	} else if layerTypeStr == "sub" {
		layerTypeSubName, ok = m.itemConf["sub_label"].(string)
		if !ok {
			layerTypeSubName = "_sub"
		}
	}

	relateSet, _ := m.itemConf["relate_document_set"]
	relateFlagStr, _ := relateSet.(string)
	if relateFlagStr == "" {
		return errors.New("关联查询必须设置关联文档集（relate_document_set）")
	}

	// 初始化imp
	rsIdv, _ := m.itemConf["relate_resource_id"]
	rsIdvStr, _ := rsIdv.(string)
	if rsIdvStr == "" {
		return errors.New("关联查询必须设置资源id（relate_resource_id）")
	}
	imp, ok := t.GetSourceImp(rsIdvStr)
	if !ok {
		return errors.New("获取关联资源错误")
	}
	m.relateImp = imp

	switch assocType.(string) {
	case "11":
		m.handle = func(op *msg.Op) (arr []*msg.Op) {
			var doc map[string]interface{}
			wheresFun := m.whereHandle(op)
			if wheresFun != nil {
				doc = (m.relateImp).RelateOneByOne(relateFlagStr, wheresFun)
			}

			// 查询为空的情况
			if len(doc) < 1 {
				//dd, _ := json.Marshal(op.Doc)
				//log.Info("一对一关联查询结果为空, 文档集为:" + relateFlagStr + ", 文档内容为:" + string(dd))
				doc = map[string]interface{}{}
			}

			// 处理关联文档的层级关系
			if layerTypeStr == "sub" { // 子级
				op.Doc[layerTypeSubName] = doc
			} else if layerTypeStr == "sib" { // 同级
				if fieldMapOrigin == nil { // 没有指定字段映射,直接合并,可能被覆盖
					op.Doc = helper.MapsMerge(op.Doc, doc)
				} else if fieldMapFun != nil { // 指定了字段,需要处理冲突字段
					op.Doc = fieldMapFun(op.Doc, doc)
				}
			}
			return []*msg.Op{op}
		}
	case "1n":
		m.handle = func(op *msg.Op) (arr []*msg.Op) {
			wheresFun := m.whereHandle(op)
			docs := (m.relateImp).RelateOneByMany(relateFlagStr, wheresFun)
			if docs == nil {
				return nil
			}
			// 处理关联文档的层级关系
			if layerTypeStr == "sub" { // 子级
				op.Doc[layerTypeSubName] = docs
				arr = []*msg.Op{op}
			} else if layerTypeStr == "sib" { // 同级
				for _, doc := range docs {
					op1Copy := *op
					if fieldMap == nil { // 没有指定冲突,直接合并,可能被覆盖
						op1Copy.Doc = helper.MapsMerge(op.Doc, doc)
					} else if fieldMapFun != nil { // 指定了字段,需要处理冲突字段
						op1Copy.Doc = fieldMapFun(op.Doc, doc)
					}
					arr = append(arr, &op1Copy)
				}
			}
			return arr
		}
	default:
		return errors.New("关联查询,参数值assoc_type值错误")
	}
	return nil
}

// fieldMapDefaultValue 默认值
func fieldMapDefaultValue(newFieldExp []string) (srcValue interface{}) {
	if len(newFieldExp) != 3 {
		return nil
	}
	fArr := newFieldExp[1:]
	// valueField => created_at|d:now 	日期格式
	// valueField => all_money|i:0.0 	浮点型
	// valueField => spread_name|str:empty  字符串
	// valueField => spread_number|i:0	整数
	if len(fArr) != 2 {
		log.Error("默认值格式错误")
		return nil
	}
	switch fArr[0] {
	case "d":
		if fArr[1] == "now" {
			srcValue = time.Now().Unix()
		} else {
			srcValue, _ = strconv.Atoi(fArr[1])
		}
	case "i":
		srcValue, _ = strconv.Atoi(fArr[1])
	case "str", "s":
		srcValue = fArr[1]
	case "f":
		srcValue, _ = strconv.ParseFloat(fArr[1], 64)
	}
	return srcValue
}

// RunningFlow 执行flow
func RunningFlow(opInput *msg.Op, t *Task) (opsOutput []*msg.Op, err error) {
	if opInput.MessageType != msg.MessageTypeDoc {
		return []*msg.Op{opInput}, nil
	}
	p, ok := d[opInput.TaskId]
	if !ok {
		return []*msg.Op{opInput}, nil
	}

	// 取出flows
	var flows []map[string]string
	flowsTemp, _ := p.flowMap[opInput.DocumentSetAlias]
	if len(flowsTemp) > 0 {
		flows = append(flows, flowsTemp...)
	}

	// 处理input
	ops := []*msg.Op{opInput}
	for _, flowItem := range flows {
		// 取 key:value
		for pipeType, idx := range flowItem {
			switch pipeType {
			case "map":
				a, ok := p.maps[idx]
				if !ok {
					continue
				}
				for _, op := range ops {
					if op.IsDel {
						continue
					}
					v, err := a.run(op.Doc, op.OptionType, op.DocumentSetAlias)
					if err != nil {
						return nil, err
					}
					if v != nil {
						if d, ok := v.(map[string]interface{}); ok {
							op.Doc = d
							// 是否重新命名主键
							didf, _ := d["__document_id_field__"]
							if val, ok := didf.(string); ok {
								op.DocIdField = val
								delete(op.Doc, "__document_id_field__")
							}
						}
					}
				}
			case "filter":
				a, ok := p.filters[idx]
				if !ok {
					continue
				}
				for _, op := range ops {
					// 由于 flow 顺序不固定,是可能先执行过了
					if op.IsDel {
						continue
					}
					v, err := a.run(op.Doc, op.OptionType, op.DocumentSetAlias)
					if err != nil {
						return nil, err
					}
					if v != nil {
						if d, ok := v.(bool); ok {
							op.IsDel = d
						}
					}
				}
			case "mapInline":
				// 需要处理一条变成多条的情况
				m, ok := p.mapInlines[idx]
				if !ok {
					continue
				}
				for _, op := range ops {
					if op.IsDel {
						continue
					}
					doc, err := m.Handle(op)
					if err != nil {
						return nil, err
					}
					op.Doc = doc
				}
			case "relateInline":
				// 需要处理一条变成多条的情况
				sa, ok := p.relateInlines[idx]
				if !ok {
					continue
				}
				// relate 需要运行时初始化一次 handle
				if sa.handle == nil {
					err := sa.SetHandle(t)
					if err != nil {
						return nil, err
					}
				}
				var arr []*msg.Op
				for _, op := range ops {
					if op.IsDel {
						continue
					}
					// 处理多条的情况
					arr1 := sa.handle(op)
					if len(arr1) > 0 {
						arr = append(arr, arr1...)
					}
				}
				if arr != nil {
					ops = arr
				}
			}
		}
	}
	return ops, nil
}

// run 需要考虑多种函数处理
func (m *Flow) run(doc map[string]interface{}, opType string, docSet string) (v interface{}, err error) {
	// otto 锁
	m.Lock.Lock()
	defer m.Lock.Unlock()

	arg := convertMapJavascript(doc)
	val, err := m.VM.Call("module.exports", nil, arg, opType, docSet)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	// map执行结果
	if val.IsObject() {
		data, _ := m.vmExportValue(val)
		dm, _ := data.(map[string]interface{})
		d3 := deepExportMap(dm)
		bytes, err := json.Marshal(d3)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		// NaN 格式化不了,转换了
		var d2 map[string]interface{}
		err = json.Unmarshal(bytes, &d2)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
		return d2, nil
	}

	// filter
	isDel, err := val.ToBoolean()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	return isDel, nil
}

// vmExportValue 导出js虚拟机的值为map类型
// 当value为object时,value.Export()的nil值会被忽略
func (m *Flow) vmExportValue(val otto.Value) (interface{}, error) {
	if !val.IsObject() {
		return val.Export()
	}

	mi := map[string]interface{}{}
	o := val.Object()
	keys := o.Keys()
	for _, key := range keys {
		v, e := o.Get(key)
		if e != nil {
			continue
		}
		if v.IsObject() && v.Class() != "Array" {
			mi[key], _ = m.vmExportValue(v)
		} else {
			mi[key], _ = v.Export()
		}
	}
	return mi, nil
}

// buildIn 构建内置函数
func (m *Flow) buildIn(t *Task) error {
	if m.VM == nil {
		return errors.New("需要初始化otto虚拟机")
	}
	funcs := []string{
		"ifnull",
		"pad",
		"trim",
		"startwith",
		"endwith",
		"strtoupper",
		"strtolower",
		"strrev",
		"strtotime",
		"date",
		"now",
		"ceil",
		"floor",
		"round",
		"rand",
		"abs",
		"max",
		"min",
		"isNumber",
	}
	err := jsvm.Lib(m.VM, funcs)
	if err != nil {
		return err
	}
	err = DocumentFind(m.VM, t)
	if err != nil {
		return err
	}
	return nil
}

// 加载外部函数
func (m *Flow) buildOut() (err error) {
	// 加载外部函数
	return nil
}

func deepExportValue(a interface{}) (b interface{}) {
	switch t := a.(type) {
	case otto.Value:
		ex, err := t.Export()
		if t.Class() == "Date" {
			ex, err = time.Parse("Mon, 2 Jan 2006 15:04:05 MST", t.String())
		}
		if err == nil {
			b = deepExportValue(ex)
		} else {
			log.Error("导出值错误: %s", err)
		}
	case map[string]interface{}:
		b = deepExportMap(t)
	case []map[string]interface{}:
		b = deepExportMapSlice(t)
	case []interface{}:
		b = deepExportSlice(t)
	case float64:
		if b == math.NaN() {
			b = float64(0)
		} else {
			b = a
		}
	default:
		b = a
	}
	return
}

func convertSliceJavascript(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = convertMapJavascript(achild)
		case []interface{}:
			avc = convertSliceJavascript(achild)
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func convertMapJavascript(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = convertMapJavascript(child)
		case []interface{}:
			o[k] = convertSliceJavascript(child)
		default:
			o[k] = v
		}
	}
	return o
}

func deepExportMapSlice(a []map[string]interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		avs = append(avs, deepExportMap(av))
	}
	return avs
}

func deepExportSlice(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		avs = append(avs, deepExportValue(av))
	}
	return avs
}

func deepExportMap(e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		o[k] = deepExportValue(v)
	}
	return o
}

// DocumentFind 条件查询文档
// js函数原型: DocumentFind(where, docSet, sourceId)
func DocumentFind(vm *otto.Otto, t *Task) error {
	return vm.Set("DocumentFind", func(call otto.FunctionCall) otto.Value {
		var wheres []map[string][2]interface{}
		value1 := call.Argument(0)
		if value1.IsObject() {
			v, _ := value1.Export()
			switch v := v.(type) {
			case []map[string]interface{}:
				for _, i := range v {
					for k, i2 := range i {
						switch i2 := i2.(type) {
						case []interface{}:
							if len(i2) == 2 {
								var i2v [2]interface{}
								for idx := 0; idx < len(i2); idx++ {
									i2v[idx] = i2[idx]
								}
								it := map[string][2]interface{}{k: i2v}
								wheres = append(wheres, it)
							}
						case []string:
							if len(i2) == 2 {
								var i2v [2]interface{}
								for idx := 0; idx < len(i2); idx++ {
									i2v[idx] = i2[idx]
								}
								it := map[string][2]interface{}{k: i2v}
								wheres = append(wheres, it)
							}
						default:
							return otto.FalseValue()
						}
					}
				}
			default:
				return otto.FalseValue()
			}
		} else {
			id, err := value1.ToString()
			if err != nil {
				return otto.FalseValue()
			}
			wheres = append(wheres, map[string][2]interface{}{"id": [2]interface{}{"=", id}})
		}

		if len(wheres) < 1 {
			return otto.FalseValue()
		}

		docSet, _ := call.Argument(1).ToString()
		resourceId, _ := call.Argument(2).ToString()
		if docSet == "" || resourceId == "" {
			return otto.FalseValue()
		}

		reader, ok := t.readerM[resourceId]
		if !ok {
			return otto.FalseValue()
		}

		doc := reader.RelateOneByOne(docSet, wheres)
		if len(doc) < 1 {
			return otto.NullValue()
		}
		result, _ := vm.ToValue(doc)
		return result
	})
}
