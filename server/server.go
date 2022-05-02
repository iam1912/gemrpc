package server

import (
	"errors"
	"fmt"
	"gemrpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

type request struct {
	h     *codec.Header
	argv  reflect.Value
	reply reflect.Value
	mtype *methodType
	svc   *service
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var invalidRequest = struct{}{}

func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined:" + s.name)
	}
	return nil
}

func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed:" + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can not find service:" + serviceName)
		return
	}
	svc, ok = svci.(*service)
	if !ok {
		err = errors.New("rpc server: cat not find service type:" + serviceName)
		return
	}
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can not find method:" + methodName)
	}
	return
}

func (server *Server) Accpet(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("rpc server: accept error:%v\n", err)
			return
		}
		go server.ServerConn(conn)
	}
}

func (server *Server) ServerConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	opt := &codec.CodecOption{}
	if err := opt.Decode(conn); err != nil {
		log.Printf("rpc server: read conn error:%v\n", err)
		return
	}
	f := codec.Codecs[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type:%v\n", opt.CodecType)
		return
	}
	server.serveCodec(f(conn), opt)
}

func (server *Server) serveCodec(cc codec.Codec, opt *codec.CodecOption) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, wg, sending, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	var h codec.Header
	err := cc.ReadHeader(&h)
	if err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("rpc server: read header error:%v\n", err)
		}
		return nil, err
	}
	req := &request{h: &h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = req.mtype.newArgv()
	req.reply = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err := cc.ReadBody(argvi); err != nil {
		log.Printf("rpc server: read body err:%v\n", err)
		return nil, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Printf("rpc server: write response error:%v\n", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, wg *sync.WaitGroup, sending *sync.Mutex, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		err := req.svc.call(req.mtype, req.argv, req.reply)
		select {
		case called <- struct{}{}:
		default:
			return
		}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.reply.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

const (
	Connected        = "200 Connected to Gem RPC"
	DefaultRPCPath   = "/_gemrpc_"
	DefaultDebugPath = "/debug/gemrpc"
)

func (server *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Printf("rpc hijacking %s: %s\n", r.RemoteAddr, err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")
	server.ServerConn(conn)
}

func (server *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, server)
}
