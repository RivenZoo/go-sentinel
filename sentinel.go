package sentinel

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"bytes"

	log "github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
)

// Sentinel provides a way to add high availability (HA) to Redis Pool using
// preconfigured addresses of Sentinel servers and name of master which Sentinels
// monitor. It works with Redis >= 2.8.12 (mostly because of ROLE command that
// was introduced in that version, it's possible though to support old versions
// using INFO command).
//
// Example of the simplest usage to contact master "mymaster":
//
//  func newSentinelPool() *redis.Pool {
//  	sntnl := &sentinel.Sentinel{
//  		Addrs:      []string{":26379", ":26380", ":26381"},
//  		MasterName: "mymaster",
//  		Dial: func(addr string) (redis.Conn, error) {
//  			timeout := 500 * time.Millisecond
//  			c, err := redis.DialTimeout("tcp", addr, timeout, timeout, timeout)
//  			if err != nil {
//  				return nil, err
//  			}
//  			return c, nil
//  		},
//  	}
//  	return &redis.Pool{
//  		MaxIdle:     3,
//  		MaxActive:   64,
//  		Wait:        true,
//  		IdleTimeout: 240 * time.Second,
//  		Dial: func() (redis.Conn, error) {
//  			masterAddr, err := sntnl.MasterAddr()
//  			if err != nil {
//  				return nil, err
//  			}
//  			c, err := redis.Dial("tcp", masterAddr)
//  			if err != nil {
//  				return nil, err
//  			}
//  			return c, nil
//  		},
//  		TestOnBorrow: func(c redis.Conn, t time.Time) error {
//  			if !sentinel.TestRole(c, "master") {
//  				return errors.New("Role check failed")
//  			} else {
//  				return nil
//  			}
//  		},
//  	}
//  }

const (
	switchMasterChannel = "+switch-master"
	defaultTimeout      = 10 // seconds
)

type Sentinel struct {
	// Addrs is a slice with known Sentinel addresses.
	Addrs []string

	// MasterName is a name of Redis master Sentinel servers monitor.
	MasterName string

	// Dial is a user supplied function to connect to Sentinel on given address. This
	// address will be chosen from Addrs slice.
	// Note that as per the redis-sentinel client guidelines, a timeout is mandatory
	// while connecting to Sentinels, and should not be set to 0.
	Dial func(addr string) (redis.Conn, error)

	// Pool is a user supplied function returning custom connection pool to Sentinel.
	// This can be useful to tune options if you are not satisfied with what default
	// Sentinel pool offers. See defaultPool() method for default pool implementation.
	// In most cases you only need to provide Dial function and let this be nil.
	Pool func(addr string) *redis.Pool

	mu    sync.RWMutex
	pools map[string]*redis.Pool
	addr  string
}

func NewSentinel(addrs []string, masterName string) *Sentinel {
	return &Sentinel{
		Addrs:      addrs,
		MasterName: masterName,
		Dial: func(addr string) (redis.Conn, error) {
			timeout := defaultTimeout * time.Second
			// read timeout set to 0 to wait sentinel notify
			c, err := redis.DialTimeout("tcp", addr,
				timeout, 0, timeout)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
}

type SentinelPool struct {
	sntl          *Sentinel
	masterWatcher *MasterSentinel
	pool          *redis.Pool
	mu            *sync.RWMutex
	curAddr       string
	closed        bool
}

func NewSentinelPool(addrs []string, masterName string,
	defaultDb int, password string) *SentinelPool {
	sp := &SentinelPool{
		sntl: NewSentinel(addrs, masterName),
		mu:   &sync.RWMutex{},
	}
	var err error
	sp.curAddr, err = sp.sntl.MasterAddr()
	if err != nil {
		panic(err)
	}
	go sp._monitorMaster()

	sp._initPool(defaultDb, password)
	return sp
}

func (sp *SentinelPool) _monitorMaster() {
	for {
		sp.mu.RLock()
		if sp.closed {
			log.Debug("sentinel pool closed")
			break
		}
		sp.mu.RUnlock()
		ms, err := sp.sntl.MasterSwitch()
		if err != nil {
			log.Errorf("subscript master switch error:%v",
				err)
		}
		w, err := ms.Watch()
		if err != nil {
			log.Errorf("watch channel error:%v",
				err)
		}
		sp.mu.Lock()
		sp.masterWatcher = ms
		sp.mu.Unlock()
		for addr := range w {
			sp.mu.Lock()
			sp.curAddr = addr
			sp.mu.Unlock()
		}
		// close in case error occured
		ms.Close()
	}
}

func (sp *SentinelPool) _initPool(defaultDb int, password string) {
	sp.pool = &redis.Pool{
		MaxIdle:     16,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			sp.mu.RLock()
			addr := sp.curAddr
			sp.mu.RUnlock()
			timeout := defaultTimeout * time.Second
			c, err := redis.DialTimeout("tcp", addr,
				timeout, timeout, timeout)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			_, selectErr := c.Do("SELECT", defaultDb)
			if selectErr != nil {
				c.Close()
				return nil, selectErr
			}
			return c, nil
		},
	}
}

// redis.Conn must Close after use
func (p *SentinelPool) Get() redis.Conn {
	return p.pool.Get()
}

func (p *SentinelPool) MasterAddr() string {
	p.mu.RLock()
	addr := p.curAddr
	p.mu.RUnlock()
	return addr
}

func (p *SentinelPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.pool.Close()
	p.masterWatcher.Close()
	p.sntl.Close()
	p.mu.Unlock()
}

// NoSentinelsAvailable is returned when all sentinels in the list are exhausted
// (or none configured), and contains the last error returned by Dial (which
// may be nil)
type NoSentinelsAvailable struct {
	lastError error
}

func (ns NoSentinelsAvailable) Error() string {
	if ns.lastError != nil {
		return fmt.Sprintf("redigo: no sentinels available; last error: %s", ns.lastError.Error())
	} else {
		return fmt.Sprintf("redigo: no sentinels available")
	}
}

// putToTop puts Sentinel address to the top of address list - this means
// that all next requests will use Sentinel on this address first.
//
// From Sentinel guidelines:
//
// The first Sentinel replying to the client request should be put at the
// start of the list, so that at the next reconnection, we'll try first
// the Sentinel that was reachable in the previous connection attempt,
// minimizing latency.
//
// Lock must be held by caller.
func (s *Sentinel) putToTop(addr string) {
	addrs := s.Addrs
	if addrs[0] == addr {
		// Already on top.
		return
	}
	newAddrs := []string{addr}
	for _, a := range addrs {
		if a == addr {
			continue
		}
		newAddrs = append(newAddrs, a)
	}
	s.Addrs = newAddrs
}

// putToBottom puts Sentinel address to the bottom of address list.
// We call this method internally when see that some Sentinel failed to answer
// on application request so next time we start with another one.
//
// Lock must be held by caller.
func (s *Sentinel) putToBottom(addr string) {
	addrs := s.Addrs
	if addrs[len(addrs)-1] == addr {
		// Already on bottom.
		return
	}
	newAddrs := []string{}
	for _, a := range addrs {
		if a == addr {
			continue
		}
		newAddrs = append(newAddrs, a)
	}
	newAddrs = append(newAddrs, addr)
	s.Addrs = newAddrs
}

// defaultPool returns a connection pool to one Sentinel. This allows
// us to call concurrent requests to Sentinel using connection Do method.
func (s *Sentinel) defaultPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   10,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return s.Dial(addr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (s *Sentinel) get(addr string) redis.Conn {
	pool := s.poolForAddr(addr)
	return pool.Get()
}

func (s *Sentinel) poolForAddr(addr string) *redis.Pool {
	s.mu.Lock()
	if s.pools == nil {
		s.pools = make(map[string]*redis.Pool)
	}
	pool, ok := s.pools[addr]
	if ok {
		s.mu.Unlock()
		return pool
	}
	s.mu.Unlock()
	newPool := s.newPool(addr)
	s.mu.Lock()
	p, ok := s.pools[addr]
	if ok {
		s.mu.Unlock()
		return p
	}
	s.pools[addr] = newPool
	s.mu.Unlock()
	return newPool
}

func (s *Sentinel) newPool(addr string) *redis.Pool {
	if s.Pool != nil {
		return s.Pool(addr)
	}
	return s.defaultPool(addr)
}

// close connection pool to Sentinel.
// Lock must be hold by caller.
func (s *Sentinel) close() {
	if s.pools != nil {
		for _, pool := range s.pools {
			pool.Close()
		}
	}
	s.pools = nil
}

func (s *Sentinel) doUntilSuccess(f func(redis.Conn) (interface{}, error)) (interface{}, error) {
	s.mu.RLock()
	addrs := s.Addrs
	s.mu.RUnlock()

	var lastErr error

	for _, addr := range addrs {
		conn := s.get(addr)
		reply, err := f(conn)
		conn.Close()
		if err != nil {
			lastErr = err
			s.mu.Lock()
			pool, ok := s.pools[addr]
			if ok {
				pool.Close()
				delete(s.pools, addr)
			}
			s.putToBottom(addr)
			s.mu.Unlock()
			continue
		}
		s.putToTop(addr)
		return reply, nil
	}

	return nil, NoSentinelsAvailable{lastError: lastErr}
}

func (s *Sentinel) subscriptMasterSwitch() (redis.PubSubConn, error) {
	s.mu.RLock()
	addrs := s.Addrs
	s.mu.RUnlock()
	var lastErr error

	for _, addr := range addrs {
		conn := s.get(addr)
		sub := redis.PubSubConn{Conn: conn}
		err := sub.Subscribe(switchMasterChannel)
		if err != nil {
			lastErr = err
			s.mu.Lock()
			pool, ok := s.pools[addr]
			if ok {
				pool.Close()
				delete(s.pools, addr)
			}
			s.putToBottom(addr)
			s.mu.Unlock()
			continue
		}
		s.putToTop(addr)
		return sub, nil
	}

	return redis.PubSubConn{nil}, NoSentinelsAvailable{lastError: lastErr}
}

type MasterSentinel struct {
	masterName string
	pubsub     redis.PubSubConn
	mu         *sync.Mutex
	closed     bool
	watchExit  chan struct{}
}

func (ms *MasterSentinel) Close() error {
	// protect pubsub.Unsubscribe to prevent concurrently called
	ms.mu.Lock()
	// prevent repeatedly call
	if ms.closed {
		return nil
	}
	ms.pubsub.Unsubscribe(switchMasterChannel)
	ms.closed = true
	// wait watch rontine exit
	<-ms.watchExit
	ms.mu.Unlock()
	return ms.pubsub.Close()
}

func (ms *MasterSentinel) Watch() (<-chan string, error) {
	ch := make(chan string)
	go func() {
		defer func() {
			close(ms.watchExit)
		}()
		for {
			switch reply := ms.pubsub.Receive().(type) {
			case redis.Message:
				p := bytes.Split(reply.Data, []byte(" "))
				if len(p) != 5 || string(p[0]) != ms.masterName {
					continue
				}
				addr := fmt.Sprintf("%s:%s", string(p[3]), string(p[4]))
				ch <- addr
			case error:
				log.Errorf("channel receive error:%v", reply)
				close(ch)
				return
			case redis.Subscription:
				if reply.Channel == switchMasterChannel &&
					reply.Kind == "unsubscribe" && reply.Count == 0 {
					log.Debugf("unsubscribe switch-master")
					close(ch)
					return
				}
			}
		}
	}()
	return ch, nil
}

func (s *Sentinel) MasterSwitch() (*MasterSentinel, error) {
	sub, err := s.subscriptMasterSwitch()
	if err != nil {
		return nil, err
	}
	return &MasterSentinel{
		pubsub:     sub,
		masterName: s.MasterName,
		closed:     false,
		mu:         &sync.Mutex{},
		watchExit:  make(chan struct{}),
	}, nil
}

// MasterAddr returns an address of current Redis master instance.
func (s *Sentinel) MasterAddr() (string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForMaster(c, s.MasterName)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// SlaveAddrs returns a slice with known slaves of current master instance.
func (s *Sentinel) SlaveAddrs() ([]string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForSlaves(c, s.MasterName)
	})
	if err != nil {
		return nil, err
	}
	return res.([]string), nil
}

// SentinelAddrs returns a slice of known Sentinel addresses Sentinel server aware of.
func (s *Sentinel) SentinelAddrs() ([]string, error) {
	res, err := s.doUntilSuccess(func(c redis.Conn) (interface{}, error) {
		return queryForSentinels(c, s.MasterName)
	})
	if err != nil {
		return nil, err
	}
	return res.([]string), nil
}

// Discover allows to update list of known Sentinel addresses. From docs:
//
// A client may update its internal list of Sentinel nodes following this procedure:
// 1) Obtain a list of other Sentinels for this master using the command SENTINEL sentinels <master-name>.
// 2) Add every ip:port pair not already existing in our list at the end of the list.
func (s *Sentinel) Discover() error {
	addrs, err := s.SentinelAddrs()
	if err != nil {
		return err
	}
	s.mu.Lock()
	for _, addr := range addrs {
		if !stringInSlice(addr, s.Addrs) {
			s.Addrs = append(s.Addrs, addr)
		}
	}
	s.mu.Unlock()
	return nil
}

// Close closes current connection to Sentinel.
func (s *Sentinel) Close() error {
	s.mu.Lock()
	s.close()
	s.mu.Unlock()
	return nil
}

// TestRole wraps GetRole in a test to verify if the role matches an expected
// role string. If there was any error in querying the supplied connection,
// the function returns false. Works with Redis >= 2.8.12.
// It's not goroutine safe, but if you call this method on pooled connections
// then you are OK.
func TestRole(c redis.Conn, expectedRole string) bool {
	role, err := getRole(c)
	if err != nil || role != expectedRole {
		return false
	}
	return true
}

// getRole is a convenience function supplied to query an instance (master or
// slave) for its role. It attempts to use the ROLE command introduced in
// redis 2.8.12.
func getRole(c redis.Conn) (string, error) {
	res, err := c.Do("ROLE")
	if err != nil {
		return "", err
	}
	rres, ok := res.([]interface{})
	if ok {
		return redis.String(rres[0], nil)
	}
	return "", errors.New("redigo: can not transform ROLE reply to string")
}

func queryForMaster(conn redis.Conn, masterName string) (string, error) {
	res, err := redis.Strings(conn.Do("SENTINEL", "get-master-addr-by-name", masterName))
	if err != nil {
		return "", err
	}
	masterAddr := strings.Join(res, ":")
	return masterAddr, nil
}

func queryForSlaves(conn redis.Conn, masterName string) ([]string, error) {
	res, err := redis.Values(conn.Do("SENTINEL", "slaves", masterName))
	if err != nil {
		return nil, err
	}
	slaves := make([]string, 0)
	for _, a := range res {
		sm, err := redis.StringMap(a, err)
		if err != nil {
			return slaves, err
		}
		slaves = append(slaves, fmt.Sprintf("%s:%s", sm["ip"], sm["port"]))
	}
	return slaves, nil
}

func queryForSentinels(conn redis.Conn, masterName string) ([]string, error) {
	res, err := redis.Values(conn.Do("SENTINEL", "sentinels", masterName))
	if err != nil {
		return nil, err
	}
	sentinels := make([]string, 0)
	for _, a := range res {
		sm, err := redis.StringMap(a, err)
		if err != nil {
			return sentinels, err
		}
		sentinels = append(sentinels, fmt.Sprintf("%s:%s", sm["ip"], sm["port"]))
	}
	return sentinels, nil
}

func stringInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
