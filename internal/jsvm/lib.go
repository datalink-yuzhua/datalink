package jsvm

import (
	c_rand "crypto/rand"
	"data-link-2.0/internal/helper"
	"fmt"
	"github.com/robertkrimen/otto"
	"math"
	"math/big"
	"strings"
	"time"
)

var Funcs = []string{
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
var funMap = map[string]func(*otto.Otto) error{
	"ifnull":     ifnull,     // 判断空
	"pad":        pad,        // 填充字符串
	"trim":       trim,       // 去掉字符串两边字符
	"startwith":  startwith,  // 检查字符串是否以指定字符开始
	"endwith":    endwith,    // 检查字符串是否以指定字符结束
	"strtoupper": strtoupper, // 字符转大写
	"strtolower": strtolower, // 字符转小写
	"strrev":     strrev,     // 字符翻转
	"strtotime":  strtotime,  // 将Y-m-d H:i:s 转为 时间戳10位
	"date":       date,       // 格式化日期
	"now":        now,        // 当前时间戳(10位)
	"ceil":       ceil,       // 向上取整
	"floor":      floor,      // 向下取整
	"round":      round,      // 四舍五入
	"rand":       rand,       // 随机数
	"abs":        abs,        // 绝对值
	"max":        max,        // 数组中的最大值
	"min":        min,        // 数组中的最小值
	"isNumber":   isNumber,   // 数字
}

// Lib 加载内置库
func Lib(vm *otto.Otto, funs []string) error {
	for _, method := range funs {
		f, ok := funMap[method]
		if !ok {
			return fmt.Errorf("method:%s not exists", method)
		}
		err := f(vm)
		if err != nil {
			return err
		}
	}
	return nil
}

// ifnull 默认值 ifnull(value, default)
func ifnull(vm *otto.Otto) error {
	return vm.Set("ifnull", func(call otto.FunctionCall) otto.Value {
		v := call.Argument(0)
		if v.IsUndefined() || v.IsNull() {
			return call.Argument(1)
		}
		// NaN 和 +INF,-INF没有考虑
		if v.IsString() {
			s, _ := v.ToString()
			if s == "" {
				return call.Argument(1)
			}
		}
		return v
	})
}

// pad 拼接字符串 pad(str,pad_char,number,direction)
func pad(vm *otto.Otto) error {
	return vm.Set("pad", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList

		direction := "l"
		switch len(params) {
		case 3:
			// pass
		case 4:
			d, _ := call.Argument(3).ToString()
			if d == "l" || d == "r" {
				direction = d
			}
		default:
			return otto.NullValue()
		}
		s, _ := call.Argument(0).ToString()
		c, _ := call.Argument(1).ToString()
		l, _ := call.Argument(2).ToInteger()
		n := int(l)
		sl := len([]rune(s))
		if sl >= n {
			result, _ := vm.ToValue(sl)
			return result
		}

		var b strings.Builder
		if direction == "l" {
			b.WriteString(helper.StrReverse(s))
		} else {
			b.WriteString(s)
		}

		for i := 0; i < n-sl; i++ {
			b.WriteString(c)
		}

		r := ""
		if direction == "l" {
			r = helper.StrReverse(b.String())
		} else {
			r = b.String()
		}
		result, _ := vm.ToValue(r)
		return result
	})
}

// trim 去掉空白字符 trim(str, ch, direction)
func trim(vm *otto.Otto) error {
	return vm.Set("trim", func(call otto.FunctionCall) otto.Value {
		l := len(call.ArgumentList)
		if l > 3 || l == 0 {
			return otto.NullValue()
		}

		var chs []rune
		v1 := call.Argument(0)
		if v1.IsNull() || v1.IsUndefined() {
			return otto.NullValue()
		}
		str, _ := v1.ToString()
		if l == 1 {
			c1 := rune(0x20) // 普通空格符
			c2 := rune(0x09) // 制表符
			c3 := rune(0x0A) // 换行符
			c4 := rune(0x0D) // 回车符
			c5 := rune(0x00) // 空字节符
			c6 := rune(0x0B) // 垂直制表符
			chs = []rune{c1, c2, c3, c4, c5, c6}
		}
		if l > 1 {
			ch, _ := call.Argument(1).ToString()
			chs = []rune(ch)
		}
		// both
		d := "b"
		if l > 2 {
			d, _ = call.Argument(2).ToString()
			if !(d == "r" || d == "l" || d == "b") {
				d = "b"
			}
		}
		switch d {
		case "r":
			str = strings.TrimRight(str, string(chs))
		case "l":
			str = strings.TrimLeft(str, string(chs))
		case "b":
			str = strings.Trim(str, string(chs))
		}
		result, _ := vm.ToValue(str)
		return result
	})
}

// startwith 检查字符串以指定字符开头 startwith(str, prefix)
func startwith(vm *otto.Otto) error {
	return vm.Set("startwith", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		str := ""
		prefix := ""
		switch len(params) {
		case 2:
			str, _ = call.Argument(0).ToString()
			prefix, _ = call.Argument(1).ToString()
		default:
			return otto.NullValue()
		}
		result, _ := vm.ToValue(strings.HasPrefix(str, prefix))
		return result
	})
}

// endwith 检查字符串以指定字符结尾 endwith(str, suffix)
func endwith(vm *otto.Otto) error {
	return vm.Set("endwith", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		str := ""
		prefix := ""
		switch len(params) {
		case 2:
			str, _ = call.Argument(0).ToString()
			prefix, _ = call.Argument(1).ToString()
		default:
			return otto.NullValue()
		}
		result, _ := vm.ToValue(strings.HasSuffix(str, prefix))
		return result
	})
}

// strtoupper 字符串大写,strtoupper(str)
func strtoupper(vm *otto.Otto) error {
	return vm.Set("strtoupper", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToString()
		result, _ := vm.ToValue(strings.ToUpper(d))
		return result
	})
}

// strtolower 字符串小写,strtolower(str)
func strtolower(vm *otto.Otto) error {
	return vm.Set("strtolower", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToString()
		result, _ := vm.ToValue(strings.ToLower(d))
		return result
	})
}

// strrev 字符串翻转,strrev(str)
func strrev(vm *otto.Otto) error {
	return vm.Set("strrev", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToString()
		a := []rune(d)
		for i := 0; i < len(a)/2; i++ {
			a[len(a)-1-i], a[i] = a[i], a[len(a)-1-i]
		}
		result, _ := vm.ToValue(string(a))
		return result
	})
}

// strtotime 时间转时间戳(10位),strtotime(str)
func strtotime(vm *otto.Otto) error {
	return vm.Set("strtotime", func(call otto.FunctionCall) otto.Value {
		layout := ""
		d, _ := call.Argument(0).ToString()
		switch len([]rune(d)) {
		case 10: // 2006-01-02
			layout = "2006-01-02"
		case 19: // 2006-01-02 15:04:05
			layout = "2006-01-02 15:04:05"
		default:
			return otto.NullValue()
		}
		t, err := time.ParseInLocation(layout, d, time.Local)
		if err != nil {
			return otto.NullValue()
		}
		result, _ := vm.ToValue(t.Unix())
		return result
	})
}

// date 格式化时间,date('Y-m-d', timestamp)
func date(vm *otto.Otto) error {
	return vm.Set("date", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		switch len(params) {
		case 2:
			f, _ := call.Argument(0).ToString()
			t, _ := call.Argument(1).ToInteger()
			switch f {
			case "Y-m-d":
				s := time.Unix(t, 0).Format("2006-01-02")
				r, _ := vm.ToValue(s)
				return r
			case "Y-m-d H:i:s":
				s := time.Unix(t, 0).Format("2006-01-02 15:04:05")
				r, _ := vm.ToValue(s)
				return r
			}
		default:
		}
		return otto.NullValue()
	})
}

// now 当前时间戳10位 now()
func now(vm *otto.Otto) error {
	return vm.Set("now", func(call otto.FunctionCall) otto.Value {
		result, _ := vm.ToValue(time.Now().Unix())
		return result
	})
}

// ceil 向上取整
func ceil(vm *otto.Otto) error {
	return vm.Set("ceil", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToFloat()
		result, _ := vm.ToValue(math.Ceil(d))
		return result
	})
}

// floor 向下取整
func floor(vm *otto.Otto) error {
	return vm.Set("floor", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToFloat()
		result, _ := vm.ToValue(math.Floor(d))
		return result
	})
}

// round 四舍五入
func round(vm *otto.Otto) error {
	return vm.Set("round", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToFloat()
		result, _ := vm.ToValue(math.Round(d))
		return result
	})
}

// rand 随机数 包含min不包含max
func rand(vm *otto.Otto) error {
	return vm.Set("rand", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		numMin := int64(0)
		numMax := int64(math.MaxInt64)
		switch len(params) {
		case 1:
			numMax, _ = call.Argument(0).ToInteger()
		case 2:
			numMin, _ = call.Argument(0).ToInteger()
			numMax, _ = call.Argument(1).ToInteger()
		default:
		}
		d, err := c_rand.Int(c_rand.Reader, big.NewInt(numMax))
		if err != nil {
			return otto.NullValue()
		}
		result, _ := vm.ToValue(numMin + d.Int64())
		return result
	})
}

// abs 绝对值
func abs(vm *otto.Otto) error {
	return vm.Set("abs", func(call otto.FunctionCall) otto.Value {
		d, _ := call.Argument(0).ToFloat()
		result, _ := vm.ToValue(math.Abs(d))
		return result
	})
}

// max 最大值
func max(vm *otto.Otto) error {
	return vm.Set("max", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		var a []float64
		switch len(params) {
		case 0:
		case 1:
			d := call.Argument(0)
			if d.IsObject() {
				v, _ := d.Object().Value().Export()
				switch v := v.(type) {
				case []int64:
					for _, v2 := range v {
						a = append(a, float64(v2))
					}
				case []float64:
					a = v
				case []interface{}:
					for _, v2 := range v {
						switch v2 := v2.(type) {
						case float64:
							a = append(a, v2)
						case int64:
							a = append(a, float64(v2))
						}
					}
				}
			}
		default:
			for _, param := range params {
				v2, _ := param.Export()
				switch v2 := v2.(type) {
				case int64:
					a = append(a, float64(v2))
				case float64:
					a = append(a, v2)
				}
			}
		}
		if len(a) < 1 {
			return otto.NullValue()
		}
		maxNum := a[0]
		for _, v := range a {
			if v > maxNum {
				maxNum = v
			}
		}
		result, _ := vm.ToValue(maxNum)
		return result
	})
}

// min 最小值
func min(vm *otto.Otto) error {
	return vm.Set("min", func(call otto.FunctionCall) otto.Value {
		params := call.ArgumentList
		var a []float64
		switch len(params) {
		case 0:
		case 1:
			d := call.Argument(0)
			if d.IsObject() {
				v, _ := d.Object().Value().Export()
				switch v := v.(type) {
				case []int64:
					for _, v2 := range v {
						a = append(a, float64(v2))
					}
				case []float64:
					a = v
				case []interface{}:
					for _, v2 := range v {
						switch v2 := v2.(type) {
						case float64:
							a = append(a, v2)
						case int64:
							a = append(a, float64(v2))
						}
					}
				}
			}
		default:
			for _, param := range params {
				v2, _ := param.Export()
				switch v2 := v2.(type) {
				case int64:
					a = append(a, float64(v2))
				case float64:
					a = append(a, v2)
				}
			}
		}
		if len(a) < 1 {
			return otto.NullValue()
		}
		minNum := a[0]
		for _, v := range a {
			if v < minNum {
				minNum = v
			}
		}
		result, _ := vm.ToValue(minNum)
		return result
	})
}

// isNumber 判断是否是数字,包含整数和浮点数
func isNumber(vm *otto.Otto) error {
	return vm.Set("isNumber", func(call otto.FunctionCall) otto.Value {
		val := call.Argument(0)
		if val.IsNumber() {
			return otto.TrueValue()
		}
		return otto.FalseValue()
	})
}
