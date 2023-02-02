package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"os/signal"
	"strings"
	//"sync"
	"syscall"
	"time"
)

var (
	//waitGrp    sync.WaitGroup
	streamName = "queue:stream"
	groupName  = "mygroup-2"
	//consumer   = "server111"
	consumer = uuid.New().String()
	counter  = 0
	poolSize = 2000
	pool     *ants.PoolWithFunc
)

func init() {
	var err error
	pool, err = ants.NewPoolWithFunc(poolSize, processQueueJob)
	if err != nil {
		panic(err.Error())
	}

}

// https://go.dev/dl/go1.20.linux-amd64.tar.gz
func main() {
	defer pool.Release()

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "x",
		Username: "queue-dev",
		Password: "x",
		DB:       0,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	creatGroup(ctx, rdb, streamName, groupName)

	consumeEvents(ctx, rdb, streamName, groupName, ">", consumer)
	err := rdb.Echo(ctx, "xxx").Err()
	if err != nil {
		panic(err)
	}

	//Gracefully disconection
	chanOS := make(chan os.Signal)
	signal.Notify(chanOS, syscall.SIGINT, syscall.SIGTERM)
	<-chanOS

	//waitGrp.Wait()
	rdb.Close()
}

func consumeEvents(ctx context.Context, client *redis.Client, streamName, consumerGroup, start, consumerName string) {

	count := 0
	//for x := 0; x < runtime.NumCPU(); x++ {
	//
	//}

	for {
		func() {
			count++
			fmt.Printf("new round %s - %d\n", time.Now().Format(time.RFC3339), count)

			streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Streams:  []string{streamName, start},
				Group:    consumerGroup,
				Consumer: consumerName,
				Count:    500,
				Block:    0,
			}).Result()

			if err != nil {
				log.Printf("err on consume events: %+v\n", err)
				return
			}

			for _, stream := range streams[0].Messages {
				//waitGrp.Add(1)

				pool.Invoke(Job{
					ctx:           ctx,
					client:        client,
					consumerGroup: consumerGroup,
					message:       stream,
				})
				//go processQueueJob(ctx, client, consumerGroup, stream)
				//counter++
				//time.Sleep(1 * time.Second)
				//go processStream(stream, false, handler.HandlerFactory())
			}
			//waitGrp.Wait()
			//fmt.Println(counter)
		}()
	} //7460

}

type Job struct {
	ctx           context.Context
	client        *redis.Client
	consumerGroup string
	message       redis.XMessage
}

// func processQueueJob(ctx context.Context, redisCli *redis.Client, consumerGroup string, message redis.XMessage) error {
func processQueueJob(args interface{}) {

	job, ok := args.(Job)
	if !ok {
		panic("expecting type Job")
	}

	//defer waitGrp.Done()

	_, ok = job.message.Values["queueName"]
	if !ok {
		panic("queueName not found")
	}
	_, ok = job.message.Values["payload"]
	if !ok {
		panic("payload not found")
	}

	//fmt.Printf("Processing item: %s => %s\n", queueName, payload)
	//time.Sleep(10 * time.Second)

	job.client.XAck(job.ctx, streamName, job.consumerGroup, job.message.ID)
	//job.client.XDel(job.ctx, streamName, job.message.ID)
}

//
//type Type string
//
//const (
//	LikeType    Type = "LikeType"
//	CommentType Type = "CommentType"
//)
//
//func processStream(stream redis.XMessage, retry bool, handlerFactory func(t Type) handler.Handler) {
//	defer waitGrp.Done()
//
//	typeEvent := stream.Values["type"].(string)
//	newEvent, _ := event.New(event.Type(typeEvent))
//
//	err := newEvent.UnmarshalBinary([]byte(stream.Values["data"].(string)))
//	if err != nil {
//		fmt.Printf("error on unmarshal stream:%v\n", stream.ID)
//		return
//	}
//
//	newEvent.SetID(stream.ID)
//
//	h := handlerFactory(newEvent.GetType())
//	err = h.Handle(newEvent, retry)
//	if err != nil {
//		fmt.Printf("error on process event:%v\n", newEvent)
//		fmt.Println(err)
//		return
//	}
//
//	//client.XDel(streamName, stream.ID)
//	client.XAck(streamName, consumerGroup, stream.ID)
//
//	//time.Sleep(2 * time.Second)
//}

func creatGroup(ctx context.Context, client *redis.Client, streamName, groupName string) {

	//if _, err := client.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Result(); err != nil {
	if _, err := client.XGroupCreate(ctx, streamName, groupName, "0").Result(); err != nil {

		if !strings.Contains(fmt.Sprint(err), "BUSYGROUP") {
			fmt.Printf("Error on create Consumer Group: %v ...\n", groupName)
			panic(err)
		}

	}
}
