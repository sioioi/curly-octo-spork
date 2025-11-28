package ws

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/example/support-mvp/internal/kafka"
	redisc "github.com/example/support-mvp/internal/redis"
)

type GatewayOptions struct {
	SessionTTL       time.Duration
	InboxBlockPeriod time.Duration
}

type GatewayHandler struct {
	redis    *redisc.Client
	producer *kafka.Producer
	opts     GatewayOptions
	upgrader websocket.Upgrader
}

func NewGatewayHandler(redis *redisc.Client, producer *kafka.Producer, opts GatewayOptions) *GatewayHandler {
	return &GatewayHandler{
		redis:    redis,
		producer: producer,
		opts:     opts,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // [CURSOR: 此处可由 AI 补全] 可追加 Origin 校验或认证逻辑。
			},
		},
	}
}

func (h *GatewayHandler) HandleSession(ctx context.Context, c *gin.Context) error {
	sessionID := c.Param("session_id")
	if sessionID == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "session_id required"})
		return errors.New("missing session id")
	}

	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return fmt.Errorf("upgrade: %w", err)
	}
	defer conn.Close()

	if err := h.redis.SetOnline(ctx, sessionID, h.opts.SessionTTL); err != nil {
		log.Printf("redis set online failed: %v", err)
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	writeErrs := make(chan error, 1)

	go h.writeLoop(connCtx, conn, sessionID, writeErrs)

	readErr := h.readLoop(connCtx, conn, sessionID)
	cancel()

	select {
	case err := <-writeErrs:
		if err != nil {
			return err
		}
	default:
	}
	return readErr
}

func (h *GatewayHandler) readLoop(ctx context.Context, conn *websocket.Conn, sessionID string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		mt, payload, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if mt != websocket.TextMessage {
			continue
		}
		if len(payload) == 0 {
			continue
		}

		if err := h.producer.Produce(ctx, sessionID, payload); err != nil {
			return fmt.Errorf("produce user message: %w", err)
		}
	}
}

func (h *GatewayHandler) writeLoop(ctx context.Context, conn *websocket.Conn, sessionID string, errs chan<- error) {
	defer close(errs)
	ticker := time.NewTicker(h.opts.SessionTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			errs <- ctx.Err()
			return
		case <-ticker.C:
			if err := h.redis.SetOnline(ctx, sessionID, h.opts.SessionTTL); err != nil {
				log.Printf("refresh online failed: %v", err)
			}
		default:
			msg, err := h.redis.BlockPopInbox(ctx, sessionID, h.opts.InboxBlockPeriod)
			if err != nil {
				errs <- fmt.Errorf("block pop inbox: %w", err)
				return
			}
			if msg == "" {
				continue
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				errs <- err
				return
			}
		}
	}
}
