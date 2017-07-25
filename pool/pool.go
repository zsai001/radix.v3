package pool

import (
	"github.com/zsai001/radix.v3/redis"
	"fmt"
)

// Pool is a simple connection pool for redis Clients. It will create a small
// pool of initial connections, and if more connections are needed they will be
// created on demand. If a connection is Put back and the pool is full it will
// be closed.
type Pool struct {
	pool chan *redis.Client
	df   DialFunc

	// The network/address that the pool is connecting to. These are going to be
	// whatever was passed into the New function. These should not be
	// changed after the pool is initialized
	Network, Addr string
}

// DialFunc is a function which can be passed into NewCustom
type DialFunc func(network, addr string) (*redis.Client, error)

// NewCustom is like New except you can specify a DialFunc which will be
// used when creating new connections for the pool. The common use-case is to do
// authentication for new connections.
func NewCustom(network, addr string, size int, df DialFunc) (*Pool, error) {
	var client *redis.Client
	var err error
	pool := make([]*redis.Client, 0, size)
	for i := 0; i < size; i++ {
		client, err = df(network, addr)
		fmt.Println("NewCustom", network, addr, size)
		if err != nil {
			for _, client = range pool {
				client.Close()
			}
			pool = pool[0:]
			break
		}
		pool = append(pool, client)
	}
	p := Pool{
		Network: network,
		Addr:    addr,
		pool:    make(chan *redis.Client, len(pool)),
		df:      df,
	}
	for i := range pool {
		p.pool <- pool[i]
	}
	return &p, err
}

// New creates a new Pool whose connections are all created using
// redis.Dial(network, addr). The size indicates the maximum number of idle
// connections to have waiting to be used at any given moment. If an error is
// encountered an empty (but still usable) pool is returned alongside that error
func New(network, addr string, size int) (*Pool, error) {
	return NewCustom(network, addr, size, redis.Dial)
}

// Get retrieves an available redis client. If there are none available it will
// create a new one on the fly
func (p *Pool) Get() (*redis.Client, error) {
	ret := <-p.pool
	if ret == nil || ret.LastCritical != nil {
		old := ret
		if ret != nil {
			go old.Close() // close old clients
		}
		ret, err := p.df(p.Network, p.Addr)
		if err != nil {
			fmt.Println("create new connection failed with", err)
		}
		return ret, err
	}
	return ret, nil
	//select {
	//case conn := <-p.pool:
	//	return conn, nil
	//default:
	//	return p.df(p.Network, p.Addr)
	//}
}

func (p *Pool) tryPut(conn *redis.Client) {
	select {
	case p.pool <- conn:
	default:
		if conn != nil {
			go conn.Close()
		}
	}
}

// Put returns a client back to the pool. If the pool is full the client is
// closed instead. If the client is already closed (due to connection failure or
// what-have-you) it will not be put back in the pool
func (p *Pool) Put(conn *redis.Client) {
	p.tryPut(conn)
	//p.pool <- conn

	//p.pool <- conn
	//if conn.LastCritical == nil {
	//	select {
	//	case p.pool <- conn:
	//	default:
	//		conn.Close()
	//	}
	//}
}

// Cmd automatically gets one client from the pool, executes the given command
// (returning its result), and puts the client back in the pool
func (p *Pool) Cmd(cmd string, args ...interface{}) *redis.Resp {
	c, err := p.Get()
	if err != nil {
		fmt.Println("error to excute", cmd, args)
		return redis.NewResp(err)
	}
	defer p.Put(c)

	return c.Cmd(cmd, args...)
}

func (p *Pool) FCmd(cmd string, args ...interface{}) *redis.Future {
	c, err := p.Get()
	defer p.Put(c)
	if err != nil {
		fmt.Println("error to excute", cmd, args)
		f := redis.NewFuture()
		f.SendResp(redis.NewRespIOErr(err))
		return f
	}
	return c.FCmd(cmd, args...)
}

// Empty removes and calls Close() on all the connections currently in the pool.
// Assuming there are no other connections waiting to be Put back this method
// effectively closes and cleans up the pool.
func (p *Pool) Empty() {
	var conn *redis.Client
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		default:
			return
		}
	}
}
