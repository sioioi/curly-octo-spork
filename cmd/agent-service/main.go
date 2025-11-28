package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/example/support-mvp/internal/kafka"
	redisc "github.com/example/support-mvp/internal/redis"
)

type agentConfig struct {
	httpAddr     string
	redisAddr    string
	kafkaBrokers []string
	userTopic    string
	groupID      string
	modeTTL      time.Duration
}

func main() {
	cfg := loadAgentConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := redisc.NewClient(cfg.redisAddr)
	defer redisClient.Close()

	consumer, err := kafka.NewConsumer(cfg.kafkaBrokers, cfg.groupID, []string{cfg.userTopic})
	if err != nil {
		log.Fatalf("kafka consumer init: %v", err)
	}
	defer consumer.Close()

	go func() {
		err := consumer.Consume(ctx, func(record kafka.Record) error {
			sessionID := string(record.Key())
			if sessionID == "" {
				sessionID = "unknown"
			}

			// [CURSOR: 此处可由 AI 补全] 可在此扩展坐席调度、优先级分配等逻辑。
			if err := redisClient.SetSessionMode(ctx, sessionID, "assigned", cfg.modeTTL); err != nil {
				log.Printf("set session mode failed: %v", err)
			} else {
				log.Printf("session %s assigned (payload=%s)", sessionID, string(record.Value()))
			}
			return nil
		})
		if err != nil {
			log.Printf("consumer stopped: %v", err)
		}
	}()

	router := gin.Default()
	router.POST("/reply", func(c *gin.Context) {
		var req struct {
			SessionID string `json:"session_id" binding:"required"`
			Text      string `json:"text" binding:"required"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := redisClient.PushInbox(c.Request.Context(), req.SessionID, req.Text); err != nil {
			log.Printf("redis push failed: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis push failed"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "queued"})
	})

	log.Printf("agent-service listening on %s", cfg.httpAddr)
	if err := router.Run(cfg.httpAddr); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gin server error: %v", err)
	}
}

func loadAgentConfig() agentConfig {
	return agentConfig{
		httpAddr:     getEnv("AGENT_HTTP_ADDR", ":8081"),
		redisAddr:    getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		kafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "127.0.0.1:9092"), ","),
		userTopic:    getEnv("KAFKA_USER_TOPIC", "user-messages"),
		groupID:      getEnv("AGENT_GROUP_ID", "agent-service"),
		modeTTL:      time.Minute * 5,
	}
}

func getEnv(key, def string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
