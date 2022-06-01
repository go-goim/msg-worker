package main

import (
	"context"

	"github.com/go-goim/core/pkg/cmd"
	"github.com/go-goim/core/pkg/log"

	messagev1 "github.com/go-goim/api/message/v1"

	"github.com/go-goim/core/pkg/graceful"
	"github.com/go-goim/core/pkg/mq"

	"github.com/go-goim/msg-worker/internal/app"
	"github.com/go-goim/msg-worker/internal/service"
)

func main() {
	if err := cmd.ParseFlags(); err != nil {
		panic(err)
	}

	application, err := app.InitApplication()
	if err != nil {
		log.Fatal("InitApplication got err", "error", err)
	}

	// register grpc
	messagev1.RegisterOfflineMessageServer(application.GrpcSrv, &service.OfflineMessageService{})

	// register consumer
	c, err := mq.NewConsumer(&mq.ConsumerConfig{
		Addr:        application.Config.SrvConfig.Mq.GetAddr(),
		Concurrence: 1,
		Subscriber:  service.GetMqMessageService(),
	})
	if err != nil {
		log.Fatal("NewConsumer got err", "error", err)
	}
	application.AddConsumer(c)

	if err = application.Run(); err != nil {
		log.Error("application run error", "error", err)
	}

	graceful.Register(application.Shutdown)
	if err = graceful.Shutdown(context.TODO()); err != nil {
		log.Error("graceful shutdown error", "error", err)
	}
}