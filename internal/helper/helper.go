package helper

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"sort"
)

// PathExists 文件是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	return false, err
}

// ReadFileByLine 读取文件
func ReadFileByLine(file string, sort string, line int) string {
	if _, err := PathExists(file); err != nil {
		return err.Error()
	}
	f, err := os.Open(file)
	if err != nil {
		return err.Error()
	}
	defer f.Close()
	if fi, _ := f.Stat(); fi.Size() == 0 {
		return "empty"
	}

	var bts []byte
	switch {
	case sort == "asc" && line == -1:
		bts, _ = ioutil.ReadAll(f)
	case sort == "asc" && line > 0:
		rb := bufio.NewReader(f)
		n, _ := LineCounter(f)
		if line > n {
			line = n
		}
		for i := 0; i < line; i++ {
			txt, _, _ := rb.ReadLine()
			bts = append(bts, txt...)
		}
	case sort == "desc":
		var idxArr []int
		buf := make([]byte, 32*1024)
		count := 0
		sep := LineSeparator()
		sepLen := len(sep)
		pos := 0
		for {
			c, err := f.Read(buf)
			if err == io.EOF || err != nil {
				break
			}
			if count == 0 {
				count += 1
				idxArr = append(idxArr, 0)
			}
			tmp := buf[:c]
			for {
				i := bytes.Index(tmp, sep)
				if i == -1 {
					break
				}
				pos += i + sepLen
				idxArr = append(idxArr, pos)
				tmp = tmp[i+sepLen:]
				count++
			}
		}
		if line <= 0 || line >= count {
			line = count
		}

		endIdx := count - line
		p := idxArr[endIdx]
		f.Seek(int64(p), 0)
		tmp, _ := ioutil.ReadAll(f)
		arr := bytes.Split(tmp, sep)
		for i := len(arr) - 1; i >= 0; i-- {
			bts = append(bts, arr[i]...)
			bts = append(bts, sep...)
		}
	}

	return string(bts)
}

func MapsMerge(maps ...map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func CopyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		vm, ok := v.(map[string]interface{})
		if ok {
			cp[k] = CopyMap(vm)
		} else {
			cp[k] = v
		}
	}
	return cp
}

// SqlEscape 转义
func SqlEscape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]

		escape = 0

		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)
}

// MapSortKeys 获取map的所有key
func MapSortKeys(m map[string]interface{}) []string {
	var keys []string
	for key, _ := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// MapKeys 获取key
func MapKeys(m map[string]interface{}) []string {
	var keys []string
	for key, _ := range m {
		keys = append(keys, key)
	}
	return keys
}

// IsNil 判断空,interface{} == nil 不能直接使用
func IsNil(obtained interface{}) (result bool) {
	if obtained == nil {
		result = true
	} else {
		switch v := reflect.ValueOf(obtained); v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return v.IsNil()
		}
	}
	return
}

// IsBoolean 是否为布尔值
func IsBoolean(v interface{}) bool {
	switch v.(type) {
	case bool:
		return true
	}
	return false
}

// IsString 是否为字符串
func IsString(v interface{}) bool {
	switch v.(type) {
	case string:
		return true
	}
	return false
}

// IsEmptyString 是否是字符串
func IsEmptyString(v interface{}) bool {
	if IsString(v) {
		if v.(string) == "" {
			return true
		}
	}
	return false
}

// IsInteger 是否是整数
func IsInteger(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	}
	return false
}

// LineSeparator 获取系统换行符
func LineSeparator() []byte {
	switch runtime.GOOS {
	case "windows":
		return []byte{'\r', '\n'}
	case "darwin":
		return []byte{'\r'}
	case "linux":
		return []byte{'\n'}
	}
	return []byte{'\n'}
}

// LineCounter 计算行
func LineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := LineSeparator()

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// ArrayReverse 数组翻转
func ArrayReverse(l []interface{}) {
	for i := 0; i < int(len(l)/2); i++ {
		li := len(l) - i - 1
		l[i], l[li] = l[li], l[i]
	}
}

// StrReverse 字符串翻转
func StrReverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
