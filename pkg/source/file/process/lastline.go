package process

import (
	"io"
	"time"

	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/file"
)

func init() {
	file.RegisterProcessor(makeLastLine)
}

func makeLastLine(config file.ReaderConfig) file.Processor {
	return &LastLineProcessor{
		inactiveTimeout: config.InactiveTimeout,
	}
}

type LastLineProcessor struct {
	inactiveTimeout time.Duration
}

func (llp *LastLineProcessor) Order() int {
	return 100
}

func (llp *LastLineProcessor) Code() string {
	return "lastLine"
}

// Process
// start->lastLine->loop->source->line->end
// LastLineProcessor 防日志采集挂起兜底processor(最后一行日志迟迟不发情况）
//
// 采集会出现一种情况, 文件已经read到了EOF, 但是最后一行的换行 LineEnd 迟迟不来(超过设置的上限时间)
// 此时, 最后一行的数据都在backlogBuffer之中，等待后续的汇聚读取
// 1.如果此前Proccessor的处理过程中,一个Line都没有发出过(ctx.WasSend == false),
//
//	直接将backlogBuffer中的这段数据发出去,并向后Seek EncodeLineEnd的长度
//	(这样处理的原因是假定程序可能会在输出下一行日志前，并不会自动换行，也就是每次输出日志，先\n再输出日志这个形态)
//
// 2.如果此前的Processor的处理过程中发出过数据, seek back backLogBuffer的长度，等待下次file的事件再发送日志。
func (llp *LastLineProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	// BacklogBuffer的作用?
	ctx.BacklogBuffer = ctx.BacklogBuffer[:0]
	// see LoopProcessor.Process()
	processorChain.Process(ctx)
	// check last line
	l := len(ctx.BacklogBuffer)
	if l <= 0 {
		return
	}
	job := ctx.Job
	// When it is necessary to back off the offset, check whether it is inactive to collect the last line
	isLastLineSend := false
	if ctx.IsEOF && !ctx.WasSend {
		if time.Since(job.LastActiveTime) >= llp.inactiveTimeout {
			// Send "last line"
			endOffset := ctx.LastOffset
			job.ProductEvent(endOffset, time.Now(), ctx.BacklogBuffer)
			job.LastActiveTime = time.Now()
			isLastLineSend = true
			// Ignore the /n that may be written next.
			// Because the "last line" of the collection thinks that either it will not be written later,
			// or it will write /n first, and then write the content of the next line,
			// it is necessary to seek a position later to ignore the /n that may be written
			_, err := job.File().Seek(int64(len(job.GetEncodeLineEnd())), io.SeekCurrent)
			if err != nil {
				log.Error("can't set offset, file(name:%s) seek error: %v", ctx.Filename, err)
			}
		} else {
			// Enable the job to escape and collect the last line
			job.EofCount = 0
		}
	}
	// Fallback accumulated buffer offset
	if !isLastLineSend {
		backwardOffset := int64(-l)
		_, err := job.File().Seek(backwardOffset, io.SeekCurrent)
		if err != nil {
			log.Error("can't set offset(%d), file(name:%s) seek error: %v", backwardOffset, ctx.Filename, err)
		}
		return
	}
}
