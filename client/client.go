package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/iam1912/gemseries/gemrpc/codec"
	"github.com/iam1912/gemseries/gemrpc/server"
)

type Call struct {
	ServiceMethod string
	Seq           uint64
	Args          interface{}
	Reply         interface{}
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	cc       codec.Codec
	opt      *codec.CodecOption
	sending  sync.Mutex
	header   codec.Header
	mu       sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
	closing  bool
	shutdown bool
}

var _ io.Closer = (*Client)(nil)

var (
	ErrShutdown = errors.New("connection is shut down")
)

func NewClient(conn net.Conn, option *codec.CodecOption) (*Client, error) {
	f := codec.Codecs[option.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type:%s", option.CodecType)
		log.Printf("rpc client: codec error:%v\n", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(option); err != nil {
		log.Printf("rpc client: options error:%v\n", err)
		conn.Close()
		return nil, err
	}
	client := &Client{
		seq:     1,
		cc:      f(conn),
		opt:     option,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client, nil
}

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body:" + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
	}
	client.header.Seq = seq
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Error = ""
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: donne channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed:" + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

func parseOptions(opts ...*codec.CodecOption) (*codec.CodecOption, error) {
	if len(opts) == 0 || opts[0] == nil {
		return &codec.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = codec.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = codec.DefaultOption.CodecType
	}
	return opt, nil
}

func Dial(network, address string, opts ...*codec.CodecOption) (client *Client, err error) {
	return DialTimeout(NewClient, network, address, opts...)
	// opt, err := parseOptions(opts...)
	// if err != nil {
	// 	return nil, err
	// }
	// conn, err := net.Dial(network, address)
	// if err != nil {
	// 	return nil, err
	// }
	// defer func() {
	// 	if client == nil {
	// 		_ = conn.Close()
	// 	}
	// }()
	// return NewClient(conn, opt)
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *codec.CodecOption) (*Client, error)

func DialTimeout(f newClientFunc, network, address string, opts ...*codec.CodecOption) (*Client, error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		select {
		case ch <- clientResult{client: client, err: err}:
		default:
			return
		}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

func NewHTTPClient(conn net.Conn, opt *codec.CodecOption) (*Client, error) {
	io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", server.DefaultRPCPath))
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == server.Connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts ...*codec.CodecOption) (*Client, error) {
	return DialTimeout(NewHTTPClient, network, address, opts...)
}

func XDial(rpcAddr string, opts ...*codec.CodecOption) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// return DialTimeout(NewClient, protocol, addr, time.Second*10, opts...)
		return Dial(protocol, addr, opts...)
	}
}
