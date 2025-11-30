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
	"github.com/example/support-mvp/internal/ws"
)

type gatewayConfig struct {
	httpAddr        string
	redisAddr       string
	kafkaBrokers    []string
	userTopic       string
	sessionTTL      time.Duration
	inboxBlockAfter time.Duration
}

func main() {
	cfg := loadGatewayConfig()

	ctx := context.Background()

	redisClient := redisc.NewClient(cfg.redisAddr)
	defer redisClient.Close()

	producer, err := kafka.NewProducer(cfg.kafkaBrokers, cfg.userTopic)
	if err != nil {
		log.Fatalf("kafka producer init: %v", err)
	}
	defer producer.Close()

	handler := ws.NewGatewayHandler(redisClient, producer, ws.GatewayOptions{
		SessionTTL:       cfg.sessionTTL,
		InboxBlockPeriod: cfg.inboxBlockAfter,
	})

	router := gin.Default()

	router.GET("/ws/:session_id", func(c *gin.Context) {
		if err := handler.HandleSession(ctx, c); err != nil {
			log.Printf("session handler error: %v", err)
		}
	})

	log.Printf("ws-gateway listening on %s", cfg.httpAddr)
	if err := router.Run(cfg.httpAddr); err != nil && err != http.ErrServerClosed {
		log.Fatalf("gin server error: %v", err)
	}
}

func loadGatewayConfig() gatewayConfig {
	// 接口地址
	// entries addr
	return gatewayConfig{
		httpAddr:  getEnv("GATEWAY_HTTP_ADDR", ":8080"),
		redisAddr: getEnv("REDIS_ADDR", "127.0.0.1:6379"),
		// 11.30审核到 kafka
		kafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "127.0.0.1:9092"), ","),

		userTopic: getEnv("KAFKA_USER_TOPIC", "user-messages"),

		sessionTTL:      time.Minute * 5,
		inboxBlockAfter: time.Second * 5,
	}
}

func getEnv(key, def string) string {
	// 允许环境变量覆盖
	// allow Envs cover
	if val := os.Getenv(key); val != "" {
		return val
	}
	return def
}
