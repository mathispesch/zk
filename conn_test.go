package zk

import (
	"context"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecurringReAuthHang(t *testing.T) {
	zkC, err := StartTestCluster(t, 3, ioutil.Discard, ioutil.Discard, nil)
	if err != nil {
		panic(err)
	}
	defer zkC.Stop()

	conn, evtC, err := zkC.ConnectAll()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)
	// Add auth.
	conn.AddAuth("digest", []byte("test:test"))

	var reauthCloseOnce sync.Once
	reauthSig := make(chan struct{}, 1)
	conn.resendZkAuthFn = func(ctx context.Context, c *Conn) error {
		// in current implimentation the reauth might be called more than once based on various conditions
		reauthCloseOnce.Do(func() { close(reauthSig) })
		return resendZkAuth(ctx, c)
	}

	conn.debugCloseRecvLoop = true
	currentServer := conn.Server()
	zkC.StopServer(currentServer)
	// wait connect to new zookeeper.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	waitForSession(ctx, evtC)

	select {
	case _, ok := <-reauthSig:
		if !ok {
			return // we closed the channel as expected
		}
		t.Fatal("reauth testing channel should have been closed")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestDeadlockInClose(t *testing.T) {
	c := &Conn{
		shouldQuit:     make(chan struct{}),
		connectTimeout: 1 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		logger:         DefaultLogger,
	}

	for i := 0; i < sendChanSize; i++ {
		c.sendChan <- &request{}
	}

	okChan := make(chan struct{})
	go func() {
		c.Close()
		close(okChan)
	}()

	select {
	case <-okChan:
	case <-time.After(3 * time.Second):
		t.Fatal("apparent deadlock!")
	}
}

// TestExpiredSession tests that if a client disconnects and does not reconnect before the session timeout,
// its session gets reset and the client correctly reconnects
func TestExpiredSession(t *testing.T) {
	ts, err := StartTestCluster(t, 3, ioutil.Discard, ioutil.Discard, nil)
	if err != nil {
		t.Fatal(err)
	}

	zk, evtC, err := ts.ConnectAll()
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zk.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = waitForSession(ctx, evtC); err != nil {
		t.Fatal("Failed to connect")
	}

	path := "/gozk-test"
	if _, err = zk.Create(path, []byte("test"), 0, WorldACL(PermAll)); err != nil {
		t.Fatalf("Create failed before restart: %+v", err)
	}

	// Change the session id so that the session is expired
	atomic.StoreInt64(&zk.sessionID, int64(1))
	zk.setState(StateDisconnected)
	zk.conn.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = waitForSession(ctx, evtC); err != nil {
		t.Fatal("Failed to reconnect")
	}

	if _, _, err = zk.Get(path); err != nil {
		t.Fatalf("Create failed after restart: %+v", err)
	}
}

// TestReconnectRestart tests that the client reconnects to a cluster that has lost its session state
// (e.g. after an instance reset or a redeploy)
func TestReconnectRestart(t *testing.T) {
	startPort := int(rand.Int31n(6000) + 10000)

	ts, err := StartTestCluster(t, 3, ioutil.Discard, ioutil.Discard, &startPort)
	if err != nil {
		t.Fatal(err)
	}

	zk, _, err := ts.ConnectAll()
	if err != nil {
		t.Fatalf("Connect returned error: %+v", err)
	}
	defer zk.Close()

	if _, err = zk.Create("/test", []byte("test"), 0, WorldACL(PermAll)); err != nil {
		t.Fatalf("Create failed before restart: %+v", err)
	}

	// Restart the cluster to erase the current session state
	err = ts.Stop()
	if err != nil {
		t.Fatal(err)
	}
	ts, err = StartTestCluster(t, 3, ioutil.Discard, ioutil.Discard, &startPort)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	if _, err = zk.Create("/test", []byte("test"), 0, WorldACL(PermAll)); err != nil {
		t.Fatalf("Create failed after restart: %+v", err)
	}
}
