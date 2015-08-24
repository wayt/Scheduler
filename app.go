package main

import (
	"flag"
	"gopkg.in/redis.v3"
	"log"
	"strconv"
	"time"
)

var redisHost = flag.String("host", "127.0.0.1:6379", "Redis server host:port")
var redisPool = flag.Int("poolsize", 10, "Redis server connection poolsize")
var redisTimeout = flag.Int("timeout", 5, "Redis server connection timeout (sec)")
var redisPassword = flag.String("password", "", "Redis server password")
var redisDB = flag.Int64("db", 0, "Redis server database")
var scheduledTasksKey = flag.String("scheduled_key", "scheduled_tasks", "Scheduled tasks redis key")
var todoTasksKey = flag.String("todo_key", "todo_tasks", "Todo tasks redis key")

func main() {
	flag.Parse()

	if *redisPool <= 0 {
		*redisPool = 10
	}

	if *redisTimeout <= 0 {
		*redisTimeout = 5
	}

	redisCli := redis.NewClient(&redis.Options{
		Addr:        *redisHost,
		Password:    *redisPassword,
		DB:          *redisDB,
		PoolSize:    *redisPool,
		PoolTimeout: time.Duration(*redisTimeout) * time.Second,
	})

	for {
		time.Sleep(1 * time.Second)

		t := time.Now()
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
}
