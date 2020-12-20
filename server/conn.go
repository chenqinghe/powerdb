package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/chenqinghe/powerdb/utils"
)

// conn is the low-level implementation of Conn
type conn struct {
	// Shared
	mu      sync.Mutex
	pending int
	err     error
	conn    net.Conn

	// Read
	readTimeout time.Duration
	br          *bufio.Reader

	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer

	// Scratch space for formatting argument length.
	// '*' or '$', length, "\r\n"
	lenScratch [32]byte

	// Scratch space for formatting integers and floats.
	numScratch [40]byte
}

type tlsHandshakeTimeoutError struct{}

func (tlsHandshakeTimeoutError) Timeout() bool   { return true }
func (tlsHandshakeTimeoutError) Temporary() bool { return true }
func (tlsHandshakeTimeoutError) Error() string   { return "TLS handshake timeout" }

// NewConn returns a new Redigo connection for the given net connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) *conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

func (c *conn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redigo: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

// Error represents an error returned in a command reply.
type Error string

func (err Error) Error() string { return string(err) }

// readLine reads a line of input from the RESP stream.
func (c *conn) readLine() ([]byte, error) {
	// To avoid allocations, attempt to read the line using ReadSlice. This
	// call typically succeeds. The known case where the call fails is when
	// reading the output from the MONITOR command.
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// The line does not fit in the bufio.Reader's buffer. Fall back to
		// allocating a buffer for the line.
		buf := append([]byte{}, p...)
		for err == bufio.ErrBufferFull {
			p, err = c.br.ReadSlice('\n')
			buf = append(buf, p...)
		}
		p = buf
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	return p[:i], nil
}

// parseLen parses bulk string and array lengths.
func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

func (r *Request) String() string {
	buf := bytes.NewBuffer([]byte(r.Command + " "))
	for _, v := range r.Args {
		buf.Write(v)
		buf.WriteString(" ")
	}

	return buf.String()
}

func (c *conn) readRequest() (*Request, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}

	if line[0] != '*' {
		return nil, protocolError("request not start with '*'")
	}

	numArgs, err := parseLen(line[1:])
	if numArgs < 0 || err != nil {
		return nil, err
	}

	args := make([][]byte, 0)

	for i := 0; i < numArgs; i++ {
		l, err := c.readLen()
		if err != nil {
			return nil, err
		}

		arg := make([]byte, l)
		_, err = io.ReadFull(c.br, arg)
		if err != nil {
			return nil, err
		}

		// read left '\r\n' in line
		if line, err := c.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}

		args = append(args, arg)
	}

	return &Request{
		Command: utils.BytesToString(args[0]),
		Args:    args[1:],
	}, nil
}

func (c *conn) readLen() (int, error) {
	line, err := c.readLine()
	if err != nil {
		return 0, err
	}
	if len(line) == 0 {
		return 0, protocolError("short response line")
	}

	if line[0] != '$' {
		return 0, protocolError("len not start with '$'")
	}

	return parseLen(line[1:])
}

type (
	StatusReply    string
	ErrorReply     string
	IntegerReply   int
	BulkReply      []byte
	MultiBulkReply [][]byte
)

func (c *conn) WriteStatusReply(s string) error {
	return c.WriteReply(StatusReply(s))
}

func (c *conn) WriteErrorReply(s string) error {
	return c.WriteReply(ErrorReply(s))
}

func (c *conn) WriteIntegerReply(n int) error {
	return c.WriteReply(IntegerReply(n))
}

func (c *conn) WriteBulkReply(p []byte) error {
	return c.WriteReply(BulkReply(p))
}

func (c *conn) WriteMultiBulkReply(p [][]byte) error {
	return c.WriteReply(MultiBulkReply(p))
}

func (c *conn) WriteOKReply() error {
	return c.WriteReply(StatusReply("OK"))
}

func (c *conn) WriteNullBulkReply() error {
	return c.WriteReply(BulkReply(nil))
}

func (c *conn) WriteReply(r interface{}) error {
	buf := bytes.NewBuffer(nil)
	var err error
	switch reply := r.(type) {
	case StatusReply:
		buf.WriteByte('+')
		buf.WriteString(string(reply))
		buf.WriteString("\r\n")
		_, err = c.bw.Write(buf.Bytes())
	case ErrorReply:
		buf.WriteByte('-')
		buf.WriteString(string(reply))
		buf.WriteString("\r\n")
		_, err = c.bw.Write(buf.Bytes())
	case IntegerReply:
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(int(reply)))
		buf.WriteString("\r\n")
		_, err = c.bw.Write(buf.Bytes())
	case BulkReply:
		buf.WriteByte('$')
		if reply == nil {
			buf.WriteString("-1")
		} else {
			buf.WriteString(strconv.Itoa(len(reply)))
			buf.WriteString("\r\n")
			buf.Write(reply)
		}
		buf.WriteString("\r\n")
		_, err = c.bw.Write(buf.Bytes())
	case MultiBulkReply:
		buf.WriteByte('*')
		buf.WriteString(strconv.Itoa(len(reply)))
		buf.WriteString("\r\n")
		_, err = c.bw.Write(buf.Bytes())
		if err != nil {
			return err
		}
		for _, v := range reply {
			if err := c.WriteReply(BulkReply(v)); err != nil {
				return err
			}
		}
	case nil:
	default:
		return errors.New("unsupported reply type:" + reflect.TypeOf(r).String())
	}

	return err
}

func (c *conn) Write(p []byte) (int, error) {
	return c.bw.Write(p)
}

func (c *conn) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.bw.Flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}
