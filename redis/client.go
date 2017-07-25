package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"
)

// ErrPipelineEmpty is returned from PipeResp() to indicate that all commands
// which were put into the pipeline have had their responses read
var ErrPipelineEmpty = errors.New("pipeline queue empty")
var ErrClientWorking = errors.New("client is working")
var ErrClientClosed = errors.New("cleint is closed")

// Client describes a Redis client.
type Client struct {
	sync.Mutex
	conn         net.Conn
	timeout      time.Duration
	pending      []request
	writeScratch []byte
	writeBuf     *bytes.Buffer

	completed, completedHead []*Resp

	// The network/address of the redis instance this client is connected to.
	// These will be whatever strings were passed into the Dial function when
	// creating this connection
	Network, Addr string

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a network error
	LastCritical error
	readChan     chan []byte
	writeChan    chan []byte
	respChan     chan *Resp
	futureChan   chan *Future
	sigClose     chan int
	wg           sync.WaitGroup
	isWorking    bool
	isClosing    bool
	send_size    int
	recv_size    int
}

// request describes a client's request to the redis server
type request struct {
	cmd  string
	args []interface{}
}

// DialTimeout connects to the given Redis server with the given timeout, which
// will be used as the read/write timeout when communicating with redis
func DialTimeout(network, addr string, timeout time.Duration) (*Client, error) {
	// establish a connection
	conn, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	fmt.Println("redis DialTimeout", addr, conn.LocalAddr().String())
	completed := make([]*Resp, 0, 10)
	client := &Client{
		Mutex:         sync.Mutex{},
		conn:          conn,
		timeout:       timeout,
		writeScratch:  make([]byte, 0, 128),
		writeBuf:      bytes.NewBuffer(make([]byte, 0, 128)),
		completed:     completed,
		completedHead: completed,
		Network:       network,
		Addr:          addr,
		readChan:      make(chan []byte, 1024),
		writeChan:     make(chan []byte, 1024),
		respChan:      make(chan *Resp, 1024),
		futureChan:    make(chan *Future, 10240),
		sigClose:      make(chan int, 1),
		isWorking:     false,
		isClosing:     false,
		wg:            sync.WaitGroup{},
	}
	client.BeginTask()
	return client, nil
}

// Dial connects to the given Redis server.
func Dial(network, addr string) (*Client, error) {
	return DialTimeout(network, addr, time.Duration(0))
}

func (c *Client) BeginTask() error {
	c.Lock()
	defer c.Unlock()

	if c.isWorking {
		return ErrClientWorking
	}
	c.isWorking = true

	go c.readProcess()
	go c.writeProcess()
	go c.respProcess()
	go c.futureProcess()
	return nil
}

func (c *Client) KeepAlive() {
	go c.keepAlive()
}

func (c *Client) keepAlive() {
	for c.isWorking && c.LastCritical == nil {
		time.Sleep(1 * time.Second)
		fresp := c.FCmd("PING")
		resp, err := fresp.TryGet(5 * time.Second) // wait 1 seconds to get the resp, otherwise closed, incase the fpop msg

		if err != nil {
			c.closeWithError(err)
			return
		}
		if resp.Err != nil {
			c.closeWithError(resp.Err)
		}
	}

	fmt.Println("keepAlive process exit", c.isWorking, c.LastCritical)
}

func (c *Client) GetAndCleanSize() (send int, recv int) {
	send = c.send_size
	recv = c.recv_size
	c.send_size = 0
	c.recv_size = 0
	return
}

func (c *Client) futureProcess() {
	c.wg.Add(1)
	defer c.wg.Done()

	defer c.closeWithError(errors.New("future process closed"))
	defer fmt.Println("futureProcess exist")
	//var max_delay int64 = 0
	//var checking = time.Now().Unix()
	for c.isWorking {
		select {
		case <-c.sigClose:
			return
		default:
			r, ok := <-c.respChan
			if !ok {
				return
			}
			f, ok := <-c.futureChan
			if !ok {
				return
			}
			f.resp <- r
			//f.outTime = time.Now().UnixNano()
			//delay := f.outTime - f.inTime
			//if max_delay < delay {
			//	max_delay = delay
			//}
			//if time.Now().Unix()-checking > 1 {
			//	fmt.Println("max delay is", float64(max_delay)/1000000.0, "ms")
			//	checking = time.Now().Unix()
			//	max_delay = 0
			//}
		}
	}
}

func (c *Client) respProcess() {
	process := NewRespReader(c)
	process.beginTask()
}

func (c *Client) readProcess() {
	c.wg.Add(1)
	defer c.wg.Done()

	defer c.closeWithError(errors.New("read process error"))

	defer fmt.Println("readProcess exist", c.LastCritical, c.conn.LocalAddr().String())
	reader := bufio.NewReaderSize(c.conn, 8192)
	buffer := make([]byte, 8192)

	for c.isWorking {
		select {
		case <-c.sigClose:
			fmt.Println("readProcess exist by sigClose")
			return
		default:
			size, err := reader.Read(buffer)
			if err != nil {
				c.LastCritical = err
				fmt.Println("readProcess exist by read error", err, c.LastCritical, c.Addr, size)
				return
			}

			send := []byte{}
			send = append(send, buffer[0:size]...)
			c.recv_size += len(send)
			c.readChan <- send
		}
	}
}

func (c *Client) writeProcess() {
	c.wg.Add(1)

	defer c.wg.Done()
	defer fmt.Println("writeProcess exist")
	maxPending := 100

	defer c.closeWithError(errors.New("write process error"))

	for c.isWorking {
		select {
		case <-c.sigClose:
			return
		default:
			send, ok := <-c.writeChan
			if !ok {
				return
			}
			data := append([]byte{}, send...)

			remain := len(c.writeChan)
			for i := 0; i < remain && i < maxPending; i++ {
				left := <-c.writeChan
				data = append(data, left...)
			}
			//fmt.Println("send with", data, string(data))
			size, err := c.conn.Write(data)
			if err != nil && size != len(data) {
				c.LastCritical = err
				go c.closeWithError(err)
				return
			}
		}
	}
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.closeWithError(c.LastCritical)
}

func (c *Client) closeWithError(reason error) error {
	go c._closeWithError(reason)
	return nil
}

func (c *Client) _closeWithError(reason error) error {
	c.Lock()
	defer c.Unlock()
	defer fmt.Println("closeWithError", c.Addr, reason)
	fmt.Println("closeWithError", c.Addr, c.conn.LocalAddr().String(), reason)
	if c.isClosing {
		return ErrClientClosed
	}

	c.isClosing = true

	//flush all cache data
	//if reason == nil {
	//	c.flushCmd()
	//}

	c.isWorking = false

	close(c.sigClose)
	close(c.writeChan)
	close(c.futureChan)
	for f := range c.futureChan {
		f.SendResp(NewRespIOErr(ErrClientClosed))
	}
	close(c.respChan)
	close(c.readChan)
	// TO-DO: need a good way to let the read routine know we want to stop
	err := c.conn.Close()
	c.wg.Wait()
	if err != nil {
		fmt.Println("client closed due to", reason)
	}
	c.LastCritical = ErrClientClosed
	fmt.Println("closeWithError", c.conn.LocalAddr().String(), c.Addr, reason)
	return err
}

// Cmd calls the given Redis command.
func (c *Client) Cmd(cmd string, args ...interface{}) *Resp {
	return c.FCmd(cmd, args...).GetResp()
}

func (c *Client) FCmd(cmd string, args ...interface{}) *Future {
	c.Lock()
	defer c.Unlock()

	f := NewFuture()
	if c.isClosing {
		f.resp <- newRespIOErr(ErrClientClosed)
		return f
	}
	err := c.writeRequest(request{cmd, args})
	if err != nil {
		f.resp <- newRespIOErr(err)
	} else {
		//f.inTime = time.Now().UnixNano()
		c.futureChan <- f
	}
	return f
}

func (c *Client) flushCmd() *Resp {
	f := NewFuture()
	err := c.writeRequest(request{"PING", nil})
	if err != nil {
		f.resp <- newRespIOErr(err)
	} else {
		c.futureChan <- f
	}
	return f.GetResp()
}

// PipeAppend adds the given call to the pipeline queue.
// Use PipeResp() to read the response.
func (c *Client) PipeAppend(cmd string, args ...interface{}) {
	c.pending = append(c.pending, request{cmd, args})
}

// PipeResp returns the reply for the next request in the pipeline queue. Err
// with ErrPipelineEmpty is returned if the pipeline queue is empty.
func (c *Client) PipeResp() *Resp {
	c.Lock()

	if len(c.completed) > 0 {
		r := c.completed[0]
		c.completed = c.completed[1:]
		c.Unlock()
		return r
	}

	if len(c.pending) == 0 {
		c.Unlock()
		return NewResp(ErrPipelineEmpty)
	}

	nreqs := len(c.pending)

	waitF := make([]*Future, nreqs)
	for i := range c.pending {
		err := c.writeRequest(c.pending[i])
		f := NewFuture()
		if err != nil {
			f.resp <- newRespIOErr(err)
		} else {
			c.futureChan <- f
		}
		waitF[i] = f
	}
	c.pending = nil

	c.completed = c.completedHead
	for i := range waitF {
		f := waitF[i]
		r := f.GetResp()
		c.completed = append(c.completed, r)
	}

	c.Unlock()
	// At this point c.completed should have something in it
	return c.PipeResp()
}

// PipeClear clears the contents of the current pipeline queue, both commands
// queued by PipeAppend which have yet to be sent and responses which have yet
// to be retrieved through PipeResp. The first returned int will be the number
// of pending commands dropped, the second will be the number of pending
// responses dropped
func (c *Client) PipeClear() (int, int) {
	callCount, replyCount := len(c.pending), len(c.completed)
	if callCount > 0 {
		c.pending = nil
	}
	if replyCount > 0 {
		c.completed = nil
	}
	return callCount, replyCount
}

// ReadResp will read a Resp off of the connection without sending anything
// first (useful after you've sent a SUSBSCRIBE command). This will block until
// a reply is received or the timeout is reached (returning the IOErr). You can
// use IsTimeout to check if the Resp is due to a Timeout
//
// Note: this is a more low-level function, you really shouldn't have to
// actually use it unless you're writing your own pub/sub code
func (c *Client) ReadResp() *Resp {
	//if c.timeout != 0 {
	//	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	//}
	return <-c.respChan
}

func (c *Client) writeRequest(requests ...request) error {
	//if c.timeout != 0 {
	//	c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	//}

	var err error
outer:
	for i := range requests {

		c.writeBuf.Reset()
		elems := flattenedLength(requests[i].args...) + 1
		_, err = writeArrayHeader(c.writeBuf, c.writeScratch, int64(elems))
		if err != nil {
			break
		}

		_, err = writeTo(c.writeBuf, c.writeScratch, requests[i].cmd, true, true)
		if err != nil {
			break
		}

		for _, arg := range requests[i].args {
			_, err = writeTo(c.writeBuf, c.writeScratch, arg, true, true)
			if err != nil {
				break outer
			}
		}

		send := append([]byte{}, c.writeBuf.Bytes()...)
		c.send_size += len(send)
		c.writeChan <- send
	}
	if err != nil {
		c.LastCritical = err
		c.closeWithError(err)
		return err
	}
	return nil
}

var errBadCmdNoKey = errors.New("bad command, no key")

// KeyFromArgs is a helper function which other library packages which wrap this
// one might find useful. It takes in a set of arguments which might be passed
// into Cmd and returns the first key for the command. Since radix supports
// complicated arguments (like slices, slices of slices, maps, etc...) this is
// not always as straightforward as it might seem, so this helper function is
// provided.
//
// An error is returned if no key can be determined
func KeyFromArgs(args ...interface{}) (string, error) {
	if len(args) == 0 {
		return "", errBadCmdNoKey
	}
	arg := args[0]
	switch argv := arg.(type) {
	case string:
		return argv, nil
	case []byte:
		return string(argv), nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			argVal := reflect.ValueOf(arg)
			if argVal.Len() < 1 {
				return "", errBadCmdNoKey
			}
			first := argVal.Index(0).Interface()
			return KeyFromArgs(first)
		case reflect.Map:
			// Maps have no order, we can't possibly choose a key out of one
			return "", errBadCmdNoKey
		default:
			return fmt.Sprint(arg), nil
		}
	}
}
