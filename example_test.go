package lossy_test

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/cevatbarisyilmaz/lossy"
)

func TestExample(t *testing.T) {
	packetConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 0,
	})
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, packetConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		log.Fatal(err)
	}
	lossyConn := lossy.NewConn(conn, 0, 25*time.Millisecond, 200*time.Millisecond, 0.02, lossy.UDPv4MinHeaderOverhead, 1500, "./traces/ATT-LTE-driving.down.txt", "./traces/ATT-LTE-driving.up.txt")
	var bytesWritten int
	const packetCount = 32
	go func() {
		for i := 0; i < packetCount; i++ {
			packet := make([]byte, i*64)
			bytesWritten += len(packet) // Ignoring the packet headers
			rand.Read(packet)
			_, err := lossyConn.Write(packet)
			if err != nil {
				log.Fatal(err)
			}
		}
		fmt.Println("Sent", packetCount, "packets with total size of", bytesWritten, "bytes")
	}()
	const timeOut = time.Second * 5
	var bytesRead int
	var packetsReceived int
	startTime := time.Now()
	for {
		err = packetConn.SetReadDeadline(time.Now().Add(timeOut))
		if err != nil {
			log.Fatal(err)
		}
		buffer := make([]byte, 32*64)
		n, addr, err := packetConn.ReadFrom(buffer)
		if err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					break
				}
			}
			log.Fatal(err)
		}
		if addr.String() != conn.LocalAddr().String() {
			log.Fatal("hijacked by", addr.String())
		}
		packetsReceived++
		bytesRead += n // Ignoring the packet headers
	}
	dur := time.Now().Sub(startTime) - timeOut
	fmt.Println("Received", packetsReceived, "packets with total size of", bytesRead, "bytes in", dur.Seconds(), "seconds")
}
