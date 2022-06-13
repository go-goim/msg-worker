package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/go-kratos/kratos/v2/selector"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	redisv8 "github.com/go-redis/redis/v8"
	ggrpc "google.golang.org/grpc"

	messagev1 "github.com/go-goim/api/message/v1"
	cgrpc "github.com/go-goim/core/pkg/conn/grpc"
	"github.com/go-goim/core/pkg/consts"
	"github.com/go-goim/core/pkg/graceful"
	"github.com/go-goim/core/pkg/log"

	"github.com/go-goim/msg-worker/internal/app"
)

type MqMessageService struct {
	rdb *redisv8.Client // remove to dao
	// grpcConnMap is a map of grpc connection. key is agentIP
	grpcConnMap sync.Map
}

var (
	mqMessageService *MqMessageService
	once             sync.Once
)

func GetMqMessageService() *MqMessageService {
	once.Do(func() {
		mqMessageService = new(MqMessageService)
		mqMessageService.rdb = app.GetApplication().Redis
	})

	return mqMessageService
}

func (s *MqMessageService) Group() string {
	return "push_msg"
}

func (s *MqMessageService) Topic() string {
	return "def_topic"
}

func (s *MqMessageService) Consume(ctx context.Context, msg ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	log.Info("Consume msg", "mqMessage", msg[0].String())
	// msg 实际上只有一条
	err := s.handleSingleMsg(ctx, msg[0])
	if err != nil {
		log.Info("consumer error", "mqMessage", string(msg[0].Body), "error", err)
	}

	return consumer.ConsumeSuccess, nil
}

func (s *MqMessageService) handleSingleMsg(ctx context.Context, msg *primitive.MessageExt) error {
	// PushMessageReq contains all MqMessage fields.
	req := &messagev1.MqMessage{}
	if err := json.Unmarshal(msg.Body, req); err != nil {
		return err
	}

	in := &messagev1.PushMessageReq{
		FromUser:        req.GetFromUser(),
		ToUser:          req.GetToUser(),
		PushMessageType: req.GetPushMessageType(),
		ContentType:     req.GetContentType(),
		Content:         req.GetContent(),
		MsgSeq:          msg.MsgId,
	}

	if req.GetPushMessageType() == messagev1.PushMessageType_Broadcast {
		return s.broadcast(ctx, in)
	}

	agentIP, err := s.rdb.Get(ctx, consts.GetUserOnlineAgentKey(req.GetToUser())).Result()
	if err != nil {
		if err == redisv8.Nil {
			log.Info("user offline, put to offline queue", "user_id", req.GetToUser())
			return s.putToRedis(ctx, msg, in)
		}
		return err
	}

	// trim port from agentIP
	agentIP = strings.Split(agentIP, ":")[0]

	log.Info("user online, send to agent", "uid", req.GetToUser(), "agent_ip", agentIP)
	cc, err := s.loadGrpcConn(ctx, agentIP)
	if err != nil {
		return err
	}

	out, err := messagev1.NewPushMessagerClient(cc).PushMessage(ctx, in)
	if err != nil {
		log.Info("MSG send msg err=", err.Error())
		return err
	}

	if !out.Success() {
		return out
	}

	return nil
}

func (s *MqMessageService) broadcast(ctx context.Context, req *messagev1.PushMessageReq) error {
	list, err := app.GetApplication().Register.GetService(ctx, app.GetApplication().Config.SrvConfig.PushService)
	if err != nil {
		return err
	}

	for _, instance := range list {
		for _, ep := range instance.Endpoints {
			if !strings.HasPrefix(ep, "grpc://") {
				continue
			}

			if err = s.broadcastToEndpoint(ctx, req, strings.TrimPrefix(ep, "grpc://")); err != nil {
				log.Info("broadcastToEndpoint err=", err)
			}
		}
	}

	return nil
}

func (s *MqMessageService) broadcastToEndpoint(ctx context.Context, req *messagev1.PushMessageReq, ep string) error {
	cc, err := grpc.DialInsecure(ctx, grpc.WithEndpoint(ep), grpc.WithTimeout(time.Second*5))
	if err != nil {
		return err
	}

	rsp, err := messagev1.NewPushMessagerClient(cc).PushMessage(ctx, req)
	if err != nil {
		return err
	}

	if !rsp.Success() {
		return rsp
	}

	return nil
}

func (s *MqMessageService) putToRedis(ctx context.Context, ext *primitive.MessageExt, req *messagev1.PushMessageReq) error {
	msgID, err := primitive.UnmarshalMsgID([]byte(ext.MsgId))
	if err != nil {
		log.Info("unmarshal ext id err=", err)
		return err
	}
	log.Info("unmarshal ext", "host", msgID.Addr, "port", msgID.Port, "offset", msgID.Offset)

	msg := &messagev1.BriefMessage{
		FromUser:    req.GetFromUser(),
		ToUser:      req.GetToUser(),
		ContentType: req.GetContentType(),
		Content:     req.GetContent(),
		MsgSeq:      ext.MsgId,
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	key := consts.GetUserOfflineQueueKey(req.GetToUser())

	// add to queue
	pp := s.rdb.Pipeline()
	_ = pp.Process(ctx, s.rdb.ZAdd(ctx, key, &redisv8.Z{
		Score:  float64(msgID.Offset),
		Member: string(body),
	}))
	// set key expire
	_ = pp.Process(ctx, s.rdb.Expire(ctx, key, consts.UserOfflineQueueKeyExpire))
	// trim old messages
	_ = pp.Process(ctx, s.rdb.ZRemRangeByRank(ctx, key, 0, -int64(consts.UserOfflineQueueMemberMax+1)))

	_, err = pp.Exec(ctx)
	if err != nil {
		log.Info("Exec pipeline err=", err)
	}

	return nil
}

func (s *MqMessageService) loadGrpcConn(_ context.Context, agentIP string) (cc *cgrpc.ClientConn, err error) {
	v, ok := s.grpcConnMap.Load(agentIP)
	if ok {
		cp := v.(*cgrpc.ConnPool)
		return cp.Get()
	}

	cp, err := cgrpc.NewConnPool(cgrpc.WithInsecure(),
		cgrpc.WithClientOption(
			grpc.WithEndpoint(fmt.Sprintf("discovery://dc1/%s", app.GetApplication().Config.SrvConfig.PushService)),
			grpc.WithDiscovery(app.GetApplication().Register),
			grpc.WithFilter(getFilter(agentIP)),
			grpc.WithTimeout(time.Second*5),
			grpc.WithOptions(ggrpc.WithBlock()),
		), cgrpc.WithPoolSize(2))
	if err != nil {
		return nil, err
	}

	s.grpcConnMap.Store(agentIP, cp)
	graceful.Register(func(_ context.Context) error {
		return cp.Release()
	})

	return cp.Get()
}

func getFilter(agentIP string) selector.Filter {
	return func(c context.Context, nodes []selector.Node) []selector.Node {
		var filtered = make([]selector.Node, 0)
		for i, n := range nodes {
			log.Debug("filter node", "agent_ip", agentIP, "node_ip", n.Address(), "scheme", n.Scheme())
			if strings.Contains(n.Address(), agentIP) {
				filtered = append(filtered, nodes[i])
				break
			}
		}

		return filtered
	}
}
