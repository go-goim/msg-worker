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
	grouppb "github.com/go-goim/api/user/group/v1"
	cgrpc "github.com/go-goim/core/pkg/conn/grpc"
	"github.com/go-goim/core/pkg/consts"
	"github.com/go-goim/core/pkg/graceful"
	"github.com/go-goim/core/pkg/log"
	"github.com/go-goim/core/pkg/waitgroup"
	"github.com/go-goim/msg-worker/internal/app"
)

type MqMessageService struct {
	rdb *redisv8.Client // remove to dao
	// grpcConnMap is a map of grpc connection. key is agentIP
	grpcConnMap   sync.Map
	userServiceCP *cgrpc.ConnPool
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

func (s *MqMessageService) handleSingleMsg(ctx context.Context, ext *primitive.MessageExt) error {
	// PushMessageReq contains all MqMessage fields.
	msg := &messagev1.Message{}
	if err := json.Unmarshal(ext.Body, msg); err != nil {
		return err
	}

	switch msg.GetSessionType() {
	case messagev1.SessionType_SingleChat:
		// push to single user
		return s.msgToUser(ctx, msg, ext)
	case messagev1.SessionType_GroupChat:
		// push to group
		return s.toGroup(ctx, msg, ext)
	case messagev1.SessionType_Broadcast:
		// push to all
		return s.broadcast(ctx, msg, ext)
	default:
		return fmt.Errorf("unknown session type: %d", msg.GetSessionType())
	}
}

func (s *MqMessageService) msgToUser(ctx context.Context, msg *messagev1.Message, ext *primitive.MessageExt) error {
	agentIP, err := s.rdb.Get(ctx, consts.GetUserOnlineAgentKey(msg.GetTo())).Result()
	if err != nil {
		if err == redisv8.Nil {
			log.Info("user offline, put to offline queue", "user_id", msg.GetTo())
			// only when user is offline, put to offline queue
			return s.putToRedis(ctx, msg.To, msg.GetMsgId(), ext.Body)
		}
		return err
	}

	// trim port from agentIP
	agentIP = strings.Split(agentIP, ":")[0]

	log.Info("user online, send to agent", "uid", msg.GetTo(), "agent_ip", agentIP)
	cc, err := s.loadGrpcConn(ctx, agentIP)
	if err != nil {
		return err
	}

	out, err := messagev1.NewPushMessageServiceClient(cc).PushMessage(ctx, &messagev1.PushMessageReq{
		Message: msg,
		ToUsers: []int64{msg.To},
	})
	if err != nil {
		log.Info("MSG send msg err=", err.Error())
		return err
	}

	if !out.Response.Success() {
		_ = s.putToRedis(ctx, msg.To, msg.MsgId, ext.Body)
		return out.Response
	}

	return nil
}

func (s *MqMessageService) putToRedis(ctx context.Context, to int64, msgID int64, body []byte) error {
	key := consts.GetUserOfflineQueueKey(to)

	// add to queue
	pp := s.rdb.Pipeline()
	_ = pp.Process(ctx, s.rdb.ZAdd(ctx, key, &redisv8.Z{
		Score:  float64(msgID),
		Member: string(body),
	}))
	// set key expire
	_ = pp.Process(ctx, s.rdb.Expire(ctx, key, consts.UserOfflineQueueKeyExpire))
	// trim old messages
	_ = pp.Process(ctx, s.rdb.ZRemRangeByRank(ctx, key, 0, -int64(consts.UserOfflineQueueMemberMax+1)))

	_, err := pp.Exec(ctx)
	if err != nil {
		log.Info("Exec pipeline err", "err", err.Error())
	}

	return nil
}

func (s *MqMessageService) broadcast(ctx context.Context, msg *messagev1.Message, _ *primitive.MessageExt) error {
	list, err := app.GetApplication().Register.GetService(ctx, app.GetApplication().Config.SrvConfig.PushService)
	if err != nil {
		return err
	}

	for _, instance := range list {
		for _, ep := range instance.Endpoints {
			if !strings.HasPrefix(ep, "grpc://") {
				continue
			}

			if err = s.broadcastToEndpoint(ctx, strings.TrimPrefix(ep, "grpc://"), &messagev1.PushMessageReq{
				Message: msg,
				ToUsers: []int64{-1},
			}); err != nil {
				log.Info("broadcastToEndpoint err=", err)
			}
		}
	}

	return nil
}

func (s *MqMessageService) broadcastToEndpoint(ctx context.Context, ep string, req *messagev1.PushMessageReq) error {
	cc, err := grpc.DialInsecure(ctx, grpc.WithEndpoint(ep), grpc.WithTimeout(time.Second*5))
	if err != nil {
		return err
	}

	rsp, err := messagev1.NewPushMessageServiceClient(cc).PushMessage(ctx, req)
	if err != nil {
		return err
	}

	if !rsp.Response.Success() {
		return rsp.Response
	}

	return nil
}

func (s *MqMessageService) toGroup(ctx context.Context, msg *messagev1.Message, ext *primitive.MessageExt) error {
	// load group members
	cc, err := s.userServiceCP.Get()
	if err != nil {
		return err
	}

	resp, err := grouppb.NewGroupServiceClient(cc).GetGroup(ctx, &grouppb.GetGroupRequest{
		Gid:         msg.GetTo(),
		WithMembers: true,
	})
	if err != nil {
		return err
	}

	if !resp.Response.Success() {
		return resp.Response
	}

	var members []int64
	for _, member := range resp.GetGroup().GetMembers() {
		if member.GetUid() == msg.GetFrom() {
			continue
		}
		members = append(members, member.Uid)
	}

	if len(members) == 0 {
		return nil
	}

	onlineUserMap, offlineUsers, err := s.filterOnlineUsers(ctx, members)
	if err != nil {
		return err
	}

	pushFunc := func(agentIP string, users []int64) error {
		cc, err := s.loadGrpcConn(ctx, agentIP)
		if err != nil {
			return err
		}

		out, err := messagev1.NewPushMessageServiceClient(cc).PushMessage(ctx, &messagev1.PushMessageReq{
			Message: msg,
			ToUsers: users,
		})
		if err != nil {
			log.Error("MSG send msg err", "err", err.Error())
			return err
		}

		if !out.Response.Success() {
			// put all to offline queue
			offlineUsers = append(offlineUsers, users...)
			return out.Response
		}

		// put failed users to offline queue
		offlineUsers = append(offlineUsers, out.GetFailedUsers()...)
		return nil
	}

	// push to online users
	wg := waitgroup.NewWaitGroup(len(onlineUserMap) + 1)
	for agentIP, users := range onlineUserMap {
		wg.Add(func() {
			if err := pushFunc(agentIP, users); err != nil {
				log.Error("push to agent err", "err", err.Error(), "agent_ip", agentIP)
			}
		})
	}
	wg.Wait()

	// maybe put redis in batch be batter
	for _, user := range offlineUsers {
		_ = s.putToRedis(ctx, user, msg.GetMsgId(), ext.Body)
	}

	return nil
}

func (s *MqMessageService) filterOnlineUsers(ctx context.Context, users []int64) (
	onlineUserMap map[string][]int64, offlineUsers []int64, err error) {

	// key is agent ip, value is user id
	onlineUserMap = make(map[string][]int64)

	for _, uid := range users {
		agentIP, err := s.rdb.Get(ctx, consts.GetUserOnlineAgentKey(uid)).Result()
		if err != nil {
			if err == redisv8.Nil {
				offlineUsers = append(offlineUsers, uid)
				continue
			}
			return nil, nil, err
		}

		// trim port from agentIP
		agentIP = strings.Split(agentIP, ":")[0]
		onlineUserMap[agentIP] = append(onlineUserMap[agentIP], uid)
	}

	return onlineUserMap, offlineUsers, nil
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
