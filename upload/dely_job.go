package upload

import (
	"api_mgr/configs"
	"context"
	"fmt"
	"os"
	"time"

	"git.yj.live/Golang/source/configmanager"
	"git.yj.live/Golang/source/log"
	"github.com/go-redis/redis/v8"
)

// StorageDelayJob 存储延迟任务
type StorageDelayJob struct {
	queue string
}

// NewStorageDelayJob .
func NewStorageDelayJob() *StorageDelayJob {
	return &StorageDelayJob{
		queue: fmt.Sprintf("%s:%s:%s:storage:delay_job_queue",
			configmanager.GetString("app", "platform"),
			configmanager.GetString("api_mgr.service.name", "api_mgr"),
			configmanager.GetString("service.metadata.tenant_name", "platform")),
	}
}

// Start .
func (s *StorageDelayJob) Start() {
	go func() {
		for {
			now := fmt.Sprintf("%d", time.Now().Unix())
			pipe := configs.RedisCli.TxPipeline()
			result := pipe.ZRangeByScore(context.Background(), s.queue, &redis.ZRangeBy{Min: "-1", Max: now})
			pipe.ZRemRangeByScore(context.Background(), s.queue, "-1", now)
			if _, err := pipe.Exec(context.Background()); err != nil {
				log.L().Errorf("fetch delay job '%s' fail[%s]", s.queue, err.Error())
			} else {
				for _, filePath := range result.Val() {
					// 删除文件
					log.L().Debugf("remove file '%s' by delay job '%s'", filePath, s.queue)
					if err := os.RemoveAll(filePath); err != nil {
						log.L().Errorf("remove file '%s' by delay job '%s' fail[%s]", filePath, s.queue, err.Error())
					}
				}
			}

			// time.Sleep(100 * time.Millisecond)
			time.Sleep(30 * time.Second)
		}
	}()
}

func (s *StorageDelayJob) delayDuration() time.Duration {
	duration, err := time.ParseDuration(configmanager.GetString("storage.delay_delete.duration", "10m"))
	if err != nil {
		duration = 10 * time.Minute
	}
	return duration
}

// Add .
func (s *StorageDelayJob) Add(filePath string) {
	log.L().Debugf("add file path '%s' into delay job '%s'", filePath, s.queue)
	if err := configs.RedisCli.ZAdd(context.Background(), s.queue, &redis.Z{Score: float64(time.Now().Add(s.delayDuration()).Unix()), Member: filePath}).Err(); err != nil {
		log.L().Errorf("add file path '%s' into delay job '%s' fail[%s]", filePath, s.queue, err.Error())
	}
}

// Remove .
func (s *StorageDelayJob) Remove(filePath string) {
	log.L().Debugf("remove file '%s' from delay job '%s'", filePath, s.queue)
	if err := configs.RedisCli.ZRem(context.Background(), s.queue, filePath).Err(); err != nil {
		log.L().Errorf("remove file '%s' from delay job '%s' fail[%s]", filePath, s.queue, err.Error())
	}
}
