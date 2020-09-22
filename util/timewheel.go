package util

import (
	"container/list"
	"time"
)

const POSTION = 0

type task struct {
	rotation int
	duration time.Duration
	callback func()
}

type TimeWheel struct {
	slotNum  int
	slotList []*list.List
	interval time.Duration
	postion  int
	ticker   *time.Ticker
	closeCh  chan bool
	taskCh   chan *task
}

func NewTimeWheel(size int, interval time.Duration) *TimeWheel {
	if size <= 0 || interval <= 0 {
		return nil
	}

	timeWheel := &TimeWheel{}
	timeWheel.slotNum = size
	timeWheel.slotList = make([]*list.List, size)
	timeWheel.interval = interval
	timeWheel.postion = POSTION
	timeWheel.closeCh = make(chan bool)
	timeWheel.taskCh = make(chan *task)
	timeWheel.initSlotList()

	return timeWheel
}

func (this *TimeWheel) initSlotList() {
	for i := range this.slotList {
		this.slotList[i] = list.New()
	}
}

func (this *TimeWheel) Start() {
	this.ticker = time.NewTicker(this.interval)
	go this.loop()
}

func (this *TimeWheel) Add(duration time.Duration, callback func()) {
	this.taskCh <- &task{0, duration, callback}
}

func (this *TimeWheel) add(task *task) {
	if task == nil {
		return
	}

	postion, rotation := this.getPostionAndRotation(task.duration)
	task.rotation = rotation
	this.slotList[postion].PushBack(task)
}

func (this *TimeWheel) getPostionAndRotation(d time.Duration) (postion, rotation int) {
	ticks := int((d + this.interval - 1) / this.interval)
	rotation = ticks / this.slotNum
	postion = (ticks + this.postion) % this.slotNum
	return postion, rotation
}

func (this *TimeWheel) handler() {
	slot := this.slotList[this.postion]
	element := slot.Front()

	for element != nil {
		t := element.Value.(*task)
		if t.rotation == 0 {
			// Asynchronous execution
			go t.callback()
			next := element.Next()
			slot.Remove(element)
			element = next
		} else {
			t.rotation--
			element = element.Next()
		}
	}
	this.postion++
	this.postion = this.postion % this.slotNum
}

func (this *TimeWheel) loop() {
Loop:
	for {
		select {
		case <-this.ticker.C:
			this.handler()
		case task := <-this.taskCh:
			this.add(task)
		case <-this.closeCh:
			close(this.taskCh)
			break Loop
		}
	}
}
