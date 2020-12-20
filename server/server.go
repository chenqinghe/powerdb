package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenqinghe/powerdb/utils"
	"github.com/sirupsen/logrus"
)

type Server struct {
	Addr string

	started uint32

	handleLock *sync.RWMutex
	handlers   map[string]HandlerFunc

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func NewServer(addr string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		Addr:       addr,
		started:    0,
		handleLock: &sync.RWMutex{},
		handlers:   make(map[string]HandlerFunc),
		ctx:        ctx,
		cancel:     cancel,
		wg:         &sync.WaitGroup{},
	}
}

type Request struct {
	Command string
	Args    [][]byte
}

type ReplyWriter interface {
	WriteReply(interface{}) error

	WriteStatusReply(s string) error

	WriteErrorReply(s string) error

	WriteIntegerReply(n int) error

	WriteBulkReply(p []byte) error

	WriteMultiBulkReply(p [][]byte) error

	WriteOKReply() error

	WriteNullBulkReply() error

	Flush() error
}

type HandlerFunc func(w ReplyWriter, req *Request)

var ErrServerClosed = errors.New("server closed")

func (s *Server) ListenAndServe() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return errors.New("server has already started")
	}

	listener, err := net.Listen("tcp4", s.Addr)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-s.ctx.Done()
		listener.Close()
	}()

	for {
		c, err := listener.Accept()
		if err != nil {
			return err
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()

			conn := NewConn(c, time.Second, time.Second)
			s.handle(s.ctx, conn)
		}()

	}
}

func (s *Server) Handle(command string, fn HandlerFunc) {
	s.handleLock.Lock()
	defer s.handleLock.Unlock()
	s.handlers[strings.ToUpper(command)] = fn
}

func (s *Server) Shutdown() error {
	if !atomic.CompareAndSwapUint32(&s.started, 1, 0) {
		return ErrServerClosed
	}
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *Server) handle(ctx context.Context, conn *conn) {
	for {
		if utils.ContextDone(ctx) {
			return
		}
		req, err := conn.readRequest()
		if err != nil {
			if err == io.EOF {
				logrus.Debugf("connection closed. remote addr: %s\r\n", conn.conn.RemoteAddr().String())
				return
			}
			logrus.Errorln("readRequest error:", err)
			conn.fatal(err)
			return
		}
		fmt.Println("request:", req.String())

		handler, ok := s.handlers[strings.ToUpper(req.Command)]
		if !ok {
			conn.WriteErrorReply(fmt.Sprintf("unknown command: %s", req.Command))
			conn.Flush()
			continue
		}

		handler(conn, req)
		conn.Flush()

	}
}
