package codec

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	MagicNumber = 0x3bef5c
)

const (
	Gob  CodecType = "appplication/gob"
	JSON CodecType = "application/json"
)

type Header struct {
	ServiceMethod string
	Seq           uint64
	Error         string
}

type NewCodecFunc func(io.ReadWriteCloser) Codec

type CodecType string

type CodecOption struct {
	CodecType      CodecType
	MagicNumber    int
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

var (
	Codecs = map[CodecType]NewCodecFunc{
		Gob: NewGobCodec,
	}
	DefaultOption = CodecOption{
		CodecType:      Gob,
		MagicNumber:    MagicNumber,
		ConnectTimeout: time.Second * 10,
	}
)

func (opt *CodecOption) Decode(r io.ReadWriteCloser) error {
	dec := json.NewDecoder(r)
	if err := dec.Decode(opt); err != nil {
		return err
	}
	if opt.MagicNumber != MagicNumber {
		return fmt.Errorf("rpc server: invalid magic number: %v\n", opt.MagicNumber)
	}
	return nil
}
