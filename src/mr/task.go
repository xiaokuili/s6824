package mr

import "time"

type Status int
type Style int

const (
	Idle        Status = 0
	In_progress Status = 1
	Completed   Status = 2
)

const (
	Map    Style = 0
	Reduce Style = 1
)

type Task struct {
	Status  Status
	Style   Style
	FileArg string
	Trigger time.Time
	// sheffle数量
	R  int
	ID int
}

func NewTask(id int, style Style) *Task {
	// FileArg, R 需要重新计算
	return &Task{
		Status: Idle,
		Style:  style,
		ID:     id,
	}
}

func EmptyTask() *Task {
	return &Task{}
}

func (t *Task) SetR(r int) {
	t.R = r
}

func (t *Task) SetTrigger() {
	t.Trigger = time.Now()
}

func (t *Task) SetFileArgs(path string) {
	t.FileArg = path
}

// 判断任务是否空闲
func (t *Task) IsIdle() bool {
	return t.Status == Idle
}

// 判断任务是否长时间不相应
func (t *Task) IsNotResponse(i int) bool {
	if t.Status == In_progress {
		return time.Now().Sub(t.Trigger) > time.Duration(i)
	}
	return false

}

// 这里省略了长期失败跳过的情况
