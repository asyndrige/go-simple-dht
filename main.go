package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
)

func main() {
	node := NewNode()
	log.Fatal(node.ListenAndServe())
}

// echo -n "hello" > /dev/udp/localhost/1055

type Node struct {
	ID   uint16
	addr string
}

func NewNode() *Node {
	// search for other nodes
	var node *Node
	for i := 1050; i < 1070; i++ {
		conn, err := net.Dial("udp", strconv.Itoa(i))
		if err != nil {
			continue
		}
		n, err := conn.Write([]byte("CONNECT"))
		if err != nil {
			continue
		}
		if n > 0 {
			node.ID = uint16(i - 1049)
			node.addr = fmt.Sprintf("localhost:%d", i)
			return node
		}
	}

	node.ID = 1
	node.addr = ":1050"
	return node
}

func (node *Node) ListenAndServe() error {
	pc, err := net.ListenPacket("udp", node.addr)
	if err != nil {
		return err
	}
	defer pc.Close()

	for {
		buf := make([]byte, 1024)
		n, addr, err := pc.ReadFrom(buf)

		log.Println(n)
		if err != nil {
			return err
		}
		go node.serve(pc, addr, buf[:n])
	}
}

func (node *Node) serve(pc net.PacketConn, addr net.Addr, buf []byte) {
	log.Printf("msg: %s", buf)
	pc.WriteTo(buf, addr)
}
