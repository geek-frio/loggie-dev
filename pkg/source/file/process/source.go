package process

import (
	"errors"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
	"io"
)

func init() {
	file.RegisterProcessor(makeSource)
}

func makeSource(config file.ReaderConfig) file.Processor {
	return &SourceProcessor{
		readBufferSize: config.ReadBufferSize,
	}
}

type SourceProcessor struct {
	readBufferSize int
}

func (sp *SourceProcessor) Order() int {
	return 300
}

func (sp *SourceProcessor) Code() string {
	return "source"
}

// Process
//
//		SourceProcessor负责从job的打开后的文件中获取新的raw readBuffer数据,
//		供后续的LineProcessor进一步处理来产生新的log line数据
//		1.任务本身也会产生新的ctx状态数据
//		  ctx.IsEOF
//		  表示文件是否被读取完毕，即返回了EOF
//		2.任务也会更新job的EofCount数据
//		 因为文件本身会发生变化,每次文件读取都是一个快照读取,读到文件末尾并不代表
//		 文件会被读取完毕,等待接收到下一次文件变化的数据的时候，文件又可以被再次读取新的数据
//	   所以EofCount会有多次的情况
func (sp *SourceProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	job := ctx.Job
	ctx.ReadBuffer = ctx.ReadBuffer[:sp.readBufferSize]
	l, err := job.File().Read(ctx.ReadBuffer)
	if errors.Is(err, io.EOF) || l == 0 {
		ctx.IsEOF = true
		job.EofCount++
		return
	}
	if err != nil {
		ctx.IsEOF = true
		log.Error("file(name:%s) read fail: %v", ctx.Filename, err)
		return
	}
	read := int64(l)
	ctx.ReadBuffer = ctx.ReadBuffer[:read]

	// see lineProcessor.Process
	processorChain.Process(ctx)
}
