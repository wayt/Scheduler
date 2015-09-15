package main

import (
	"flag"
	"fmt"
	"gopkg.in/redis.v3"
	"log"
	"strconv"
	"time"
)

var redisHost = flag.String("host", "127.0.0.1:6379", "Redis server host:port")
var redisPool = flag.Int("poolsize", 10, "Redis server connection poolsize")
var redisTimeout = flag.Int("timeout", 5, "Redis server connection timeout (sec)")
var redisPassword = flag.String("password", "", "Redis server password")
var redisDBs redisDBList
var scheduledTasksKey = flag.String("scheduled_key", "scheduled_tasks", "Scheduled tasks redis key")
var todoTasksKey = flag.String("todo_key", "todo_tasks", "Todo tasks redis key")

func init() {
	flag.Var(&redisDBs, "db", "Redis server database")
}

type redisDBList []int64

func (l *redisDBList) String() string {
	return fmt.Sprint(*l)
}

func (l *redisDBList) Set(value string) error {

	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}

	*l = append(*l, val)
	return nil
}

func main() {
	flag.Parse()

	if len(redisDBs) == 0 { // add default database
		redisDBs = append(redisDBs, 0)
	}

	if *redisPool <= 0 {
		*redisPool = 10
	}

	if *redisTimeout <= 0 {
		*redisTimeout = 5
	}

	ch := make(chan error)
	for _, db := range redisDBs {

		log.Println("Running on db:", db)

		go func(db int64) {
			redisCli := redis.NewClient(&redis.Options{
				Addr:        *redisHost,
				Password:    *redisPassword,
				DB:          db,
				PoolSize:    *redisPool,
				PoolTimeout: time.Duration(*redisTimeout) * time.Second,
			})

			for {
				time.Sleep(1 * time.Second)

				t := time.Now().UTC()
				timeStr := strconv.FormatInt(t.Unix(), 10)

				tasks, err := redisCli.ZRangeByScore(*scheduledTasksKey, redis.ZRangeByScore{
					Min: "-inf",
					Max: timeStr,
				}).Result()

				if err != nil {
					log.Println("redisCli.ZRangeByScore:", err)
					continue
				}

				if len(tasks) == 0 {
					log.Println("No tasks...")
					continue
				}

				if err := redisCli.RPush(*todoTasksKey, tasks...).Err(); err != nil {
					log.Println("redisCli.RPush:", err)
					continue
				}

				if err := redisCli.ZRemRangeByScore(*scheduledTasksKey, "-inf", timeStr).Err(); err != nil {
					log.Println("redisCli.ZRemRangeByScore:", err)
					continue
				}

				log.Println("scheduled", len(tasks), "task(s).")
			}
		}(db)
	}

	<-ch
}
