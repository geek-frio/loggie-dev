/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package file

import (
	"sync"
	"time"

	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const (
	SystemStateKey = event.SystemKeyPrefix + "State"
)

type State struct {
	Epoch        *pipeline.Epoch `json:"-"`
	PipelineName string          `json:"-"`
	SourceName   string          `json:"-"`
	Offset       int64           `json:"offset"`
	NextOffset   int64           `json:"nextOffset"`
	Filename     string          `json:"filename,omitempty"`
	CollectTime  time.Time       `json:"collectTime,omitempty"`
	ContentBytes int64           `json:"contentBytes"`
	JobUid       string          `json:"jobUid,omitempty"`
	JobIndex     uint32          `json:"-"`
	EventUid     string          `json:"-"`
	LineNumber   int64           `json:"lineNumber,omitempty"`
	Tags         string          `json:"tags,omitempty"`

	// for cache
	// watchJobId的值: pipelineName:sourceName:jobUid
	watchUid string

	// jobFields from job
	jobFields map[string]interface{}
}

// WatchUid
// pipelineName:sourceName:jobUid
func (s *State) WatchUid() string {
	return s.watchUid
}

func (s *State) AppendTags(tag string) {
	if s.Tags == "" {
		s.Tags = tag
	} else {
		s.Tags = s.Tags + "," + tag
	}
}

type Reader struct {
	done      chan struct{}
	config    ReaderConfig
	jobChan   chan *Job
	watcher   *Watcher
	countDown *sync.WaitGroup
	stopOnce  *sync.Once
	startOnce *sync.Once
}

func newReader(config ReaderConfig, watcher *Watcher) *Reader {
	r := &Reader{
		done:      make(chan struct{}),
		config:    config,
		jobChan:   make(chan *Job, config.readChanSize),
		watcher:   watcher,
		countDown: &sync.WaitGroup{},
		stopOnce:  &sync.Once{},
		startOnce: &sync.Once{},
	}
	r.Start()
	return r
}

func (r *Reader) Stop() {
	r.stopOnce.Do(func() {
		close(r.done)
		r.countDown.Wait()
		go r.cleanData()
	})
}

func (r *Reader) cleanData() {
	timeout := time.NewTimer(r.config.CleanDataTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			return
		case j := <-r.jobChan:
			r.watcher.decideJob(j)
		}
	}
}

func (r *Reader) Start() {
	r.startOnce.Do(func() {
		for i := 0; i < r.config.WorkerCount; i++ {
			index := i
			go r.work(index)
		}
	})
}

// work
// 有多个worker在执行, index 作为Worker的下标
func (r *Reader) work(index int) {
	r.countDown.Add(1)
	log.Info("read worker-%d start", index)
	defer func() {
		log.Info("read worker-%d stop", index)
		r.countDown.Done()
	}()
	readBufferSize := r.config.ReadBufferSize
	// 当一行的数据的长度超过readBuffer的时候,数据会暂存在backlogBuffer之中
	//   readBuffer会重新从文件中拉取对应的readBuffer长度的数据,试图找到LineEnd分割符
	backlogBuffer := make([]byte, 0, readBufferSize)
	readBuffer := make([]byte, readBufferSize)
	jobs := r.jobChan
	processChain := r.buildProcessChain()
	for {
		select {
		case <-r.done:
			return
			// Job 应该来自于对于指定路径文件系统的监控, 发现需要监控的文件的时候,这里会产生新的job任务
		case job := <-jobs:
			if ctx, err := NewJobCollectContextAndValidate(job, readBuffer, backlogBuffer); err == nil {
				processChain.Process(ctx)
			}
			r.watcher.decideJob(job)
		}
	}
}

func (r *Reader) buildProcessChain() ProcessChain {
	return NewProcessChain(r.config)
}
