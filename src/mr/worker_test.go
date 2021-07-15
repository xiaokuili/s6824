package mr

import (
	"encoding/json"
	"log"
	"os"
	"sort"
	"testing"
)

func runReduce(filenames []string) {
	// 读取文件
	// 执行函数
	// 写入文件

	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		if filename == "" {
			break
		}
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()

	}
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// this is the correct format for each line of Reduce output.

		i = j
	}

}

func Test_mrWorker_runReduce(t *testing.T) {
	type args struct {
		t       *Task
		reducef func(string, []string) string
	}
	tests := []struct {
		name string
		mr   *mrWorker
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := &mrWorker{}
			if got := mr.runReduce(tt.args.t, tt.args.reducef); got != tt.want {
				t.Errorf("mrWorker.runReduce() = %v, want %v", got, tt.want)
			}
		})
	}
}
