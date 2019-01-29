package main

import (
	"crypto/md5"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var _ = md5.Sum
var startScanPort = 1049

// verbs
const (
	connect = "CONNECT"
	success = "OK"
	ping    = "PING"
	synchro = "SYNC"
)

func main() {
	if node := NewNode(); node != nil {
		log.Fatal(node.ListenAndServe())
	}
}

// echo -n "hello" > /dev/udp/localhost/1055

type Node struct {
	ID       uint16
	addr     string
	conn     net.PacketConn
	ht       *HashTable
	pingChan chan uint16
	// next *Node
	// prev *Node
}

func NewNode() *Node {
	// search for other nodes

	for i := 1; i < 21; i++ {
		addr := fmt.Sprintf("localhost:%d", startScanPort+i)
		conn, err := net.ListenPacket("udp", addr)
		if err != nil {
			continue
		}
		return &Node{
			ID:       uint16(i),
			conn:     conn,
			addr:     addr,
			ht:       NewHashTable(),
			pingChan: make(chan uint16),
		}
	}

	return nil
}

func (node *Node) String() string {
	return fmt.Sprintf("node id: %d addr: %s", node.ID, node.addr)
}

func (node *Node) ListenAndServe() error {
	defer node.conn.Close()
	node.ht.Set(node.ID, node.addr)
	node.broadcastConnect()

	log.Printf("%s start", node)
	// notifyChan := make(chan os.Signal)
	// signal.Notify(notifyChan, os.Interrupt, syscall.SIGTERM)
	refresh := time.NewTicker(10 * time.Second).C

	go func() {
		for {
			select {
			// case <-notifyChan:
			// 	os.Exit(1)
			case <-refresh:
				// sync data
				node.pingNodes()
				log.Printf("current ht data: %s", node.ht)
				// case id := <-node.pingChan:
				// 	awaitPingBack := time.NewTicker(15*time.Second).C
				// 	go func() {
				// 		case <-awaitPingBack
				// 	}
			}

		}
	}()

	for {
		buf := make([]byte, 1024)
		n, addr, err := node.conn.ReadFrom(buf)
		if err != nil {
			return err
		}

		log.Println(n)
		go node.serve(node.conn, addr, buf[:n])
	}
}

func (node *Node) serve(pc net.PacketConn, addr net.Addr, buf []byte) {
	log.Printf("msg: %s", buf)

	msg := string(buf)
	if strings.HasPrefix(msg, connect) {
		id, a, _ := extractData(msg)
		node.ht.Set(uint16(id), a)
	} else if strings.HasPrefix(msg, ping) {
		id, a, _ := extractData(msg)
		node.ht.Set(id, a)
	}

	// pc.WriteTo(buf, addr)
}

func (node *Node) broadcastConnect() {
	log.Printf("start broadcasting from %s", node)
	wg := new(sync.WaitGroup)

	for i := 0; i < 21; i++ {
		addr := fmt.Sprintf("localhost:%d", startScanPort+i)
		if addr == node.addr {
			continue
		}

		wg.Add(1)
		go func(addr string, wg *sync.WaitGroup) {
			defer wg.Done()
			conn, err := net.Dial("udp", addr)
			if err != nil {
				return
			}
			_, err = conn.Write([]byte(fmt.Sprintf("%s %d %s", connect, node.ID, node.addr)))
			if err != nil {
				return
			}
		}(addr, wg)
	}

	wg.Wait()
}

func (node *Node) pingNodes() {
	log.Printf("start ping other nodes")

	wg := new(sync.WaitGroup)
	for id, addr := range node.ht.data {
		wg.Add(1)
		go func(id uint16, mu *sync.WaitGroup) {
			defer wg.Done()

			conn, err := net.Dial("udp", addr)
			if err != nil {
				println(err.Error(), id)
				return
			}
			_, err = conn.Write([]byte(fmt.Sprintf("%s %d %s", ping, node.ID, node.addr)))
			if err != nil {
				println(err.Error(), id)
				return
			}
		}(id, wg)
	}
	wg.Wait()
}

type HashTable struct {
	mu   *sync.Mutex
	data map[uint16]string
}

func NewHashTable() *HashTable {
	return &HashTable{
		mu:   new(sync.Mutex),
		data: make(map[uint16]string),
	}
}

func (ht *HashTable) String() string {
	return fmt.Sprintf("%#v\n", ht.data)
}

func (ht *HashTable) withLockContext(fn func()) {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	fn()
}

func (ht *HashTable) Get(id uint16) (val string, ok bool) {
	ht.withLockContext(func() {
		val, ok = ht.data[id]
	})
	return
}

func (ht *HashTable) Set(id uint16, val string) {
	ht.withLockContext(func() {
		ht.data[id] = val // hash
	})
}

func (ht *HashTable) Del(id uint16, val string) {
	ht.withLockContext(func() {
		delete(ht.data, id)
	})
}

func extractData(msg string) (id uint16, addr string, err error) {
	msgSplit := strings.Split(msg, " ")
	if len(msgSplit) > 2 {
		var idInt int
		idInt, err = strconv.Atoi(msgSplit[1])
		if err != nil {
			// node.ht.Set(uint16(id), msgSplit[2])
			return
		}

		return uint16(idInt), msgSplit[2], nil
	}
	return
}
