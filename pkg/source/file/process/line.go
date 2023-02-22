package process

import (
	"bytes"
	"time"

	"github.com/loggie-io/loggie/pkg/source/file"
)

// init
// 所有的包都可以包含一个或者多个init函数, 当一个包被导入的时候, 所有的init函数都会被自动执行
// 所有包之间的init函数执行顺序无法保证
func init() {
	file.RegisterProcessor(makeLine)
}

func makeLine(config file.ReaderConfig) file.Processor {
	return &LineProcessor{}
}

type LineProcessor struct {
}

func (lp *LineProcessor) Order() int {
	return 500
}

func (lp *LineProcessor) Code() string {
	return "line"
}

// Process
// Process Logic:
//
//	start->lastLine->loop->source->*line*->end
//
// ReadBuffer 是从文件截取出的一块buffer数据,进行Line的获取操作
func (lp *LineProcessor) Process(processorChain file.ProcessChain, ctx *file.JobCollectContext) {
	job := ctx.Job
	now := time.Now()
	readBuffer := ctx.ReadBuffer
	read := int64(len(readBuffer))
	processed := int64(0)
	// 只要处理的字节没有超过readBuffer中剩余的字节,
	// 那么Buffer中的Line截取操作就会一直执行下去
	for processed < read {
		index := int64(bytes.Index(readBuffer[processed:], job.GetEncodeLineEnd()))
		// 这个逻辑对应于readBuffer中从processed下标开始后一个行分隔符都没有的情况,
		//  需要重新读取下一个buffer(当前这个buffer中processed开始到buffer结束的
		//  字节数据都应该作为一行中的部分数据)
		// 此时需要将这部分数据暂存, 放入BackLogBuffer
		if index == -1 {
			break
		}
		index += processed

		endOffset := ctx.LastOffset + index
		if len(ctx.BacklogBuffer) != 0 {
			ctx.BacklogBuffer = append(ctx.BacklogBuffer, readBuffer[processed:index]...)
			job.ProductEvent(endOffset, now, ctx.BacklogBuffer)

			// Clean the backlog buffer after sending
			ctx.BacklogBuffer = ctx.BacklogBuffer[:0]
		} else {
			buffer := readBuffer[processed:index]
			if len(buffer) > 0 {
				job.ProductEvent(endOffset, now, buffer)
			}
		}
		processed = index + int64(len(job.GetEncodeLineEnd()))
	}
	ctx.LastOffset += read
	ctx.WasSend = processed != 0
	// The remaining bytes read are added to the backlog buffer
	if processed < read {
		ctx.BacklogBuffer = append(ctx.BacklogBuffer, readBuffer[processed:]...)

		// TODO check whether it is too long to avoid bursting the memory
		//if len(backlogBuffer)>max_bytes{
		//	log.Error
		//	break
		//}
	}
}
