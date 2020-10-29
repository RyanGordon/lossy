package lossy

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

type conn struct {
	net.Conn
	minLatency        time.Duration
	maxLatency        time.Duration
	packetLossRate    float64
	writeDeadline     time.Time
	closed            bool
	closer            chan bool
	mu                *sync.Mutex
	rand              *rand.Rand
	throttleMu        *sync.Mutex
	timeToWaitPerByte float64
	headerOverhead    int
	mtuSize           int

	// Sim Read
	simUpInitialTime time.Time
	simUp            map[int]int
	simUpOrdered     []int
	simMaxUpMs       int64
	upQueue          chan []byte

	// Sim Down
	simDownInitialTime    time.Time
	simDown               map[int]int
	simDownOrdered        []int
	simMaxDownMs          int64
	intermediateDownQueue chan []byte
	downQueue             chan []byte
}

// NewConn wraps the given net.Conn with a lossy connection.
//
// bandwidth is in bytes/second.
// i.e. enter 1024 * 1024 for a 8 Mbit/s connection.
// Enter 0 or a negative value for an unlimited bandwidth.
//
// minLatency and maxLatency is used to create a random latency for each packet.
// maxLatency should be equal or greater than minLatency.
// If bandwidth is not unlimited and there's no other packets waiting to be delivered,
// time to deliver a packet is (len(packet) + headerOverhead) / bandwidth + randomDuration(minLatency, maxLatency)
//
// packetLossRate is chance of a packet to be dropped.
// It should be less than 1 and equal or greater than 0.
//
// headerOverhead is the header size of the underlying protocol of the connection.
// It is used to simulate bandwidth more realistically.
// If bandwidth is unlimited, headerOverhead is ignored.
func NewConn(c net.Conn, bandwidth int, minLatency, maxLatency time.Duration, packetLossRate float64, headerOverhead int, mtuSize int, simUpFile string, simDownFile string) net.Conn {
	var timeToWaitPerByte float64
	if bandwidth <= 0 {
		timeToWaitPerByte = 0
	} else {
		timeToWaitPerByte = float64(time.Second) / float64(bandwidth)
	}

	simUp, simUpOrdered, simMaxUpMs, err := readSaturatorTrace(simUpFile)
	if err != nil {
		panic(err)
	}

	simDown, simDownOrdered, simMaxDownMs, err := readSaturatorTrace(simDownFile)
	if err != nil {
		panic(err)
	}

	netConn := &conn{
		Conn:              c,
		minLatency:        minLatency,
		maxLatency:        maxLatency,
		packetLossRate:    packetLossRate / 2.0, // divide by 2 because it applies in both directions
		writeDeadline:     time.Time{},
		closer:            make(chan bool, 1),
		closed:            false,
		mu:                &sync.Mutex{},
		rand:              rand.New(rand.NewSource(time.Now().UnixNano())),
		throttleMu:        &sync.Mutex{},
		timeToWaitPerByte: timeToWaitPerByte,
		headerOverhead:    headerOverhead,
		mtuSize:           mtuSize,

		simUpInitialTime: time.Now(),
		simUp:            simUp,
		simUpOrdered:     simUpOrdered,
		simMaxUpMs:       simMaxUpMs,
		upQueue:          make(chan []byte, 100000),

		simDownInitialTime:    time.Now(),
		simDown:               simDown,
		simDownOrdered:        simDownOrdered,
		simMaxDownMs:          simMaxDownMs,
		intermediateDownQueue: make(chan []byte, 100000),
		downQueue:             make(chan []byte, 100000),
	}

	if len(netConn.simDown) > 0 {
		go netConn.backgroundSimIntermediateReader()
		go netConn.backgroundSimReader()
	} else {
		go netConn.backgroundReader()
	}

	if len(netConn.simUp) > 0 {
		go netConn.backgroundSimWriter()
	}

	return netConn
}

func (c *conn) Read(b []byte) (int, error) {
	bCap := cap(b)

	var read []byte
	select {
	case read = <-c.downQueue:
	case <-c.closer:
		return 0, fmt.Errorf("Conn closed")
	}

	readLen := len(read)
	if bCap < readLen {
		readLen = bCap
	}

	for i := 0; i < readLen; i++ {
		b[i] = read[i]
	}

	return readLen, nil
}

func (c *conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || !c.writeDeadline.Equal(time.Time{}) && c.writeDeadline.Before(time.Now()) {
		return c.Conn.Write(b)
	}

	if len(c.simUp) > 0 {
		select {
		case c.upQueue <- b:
			return len(b), nil
		default:
			return 0, errors.New("Write queue full")
		}
	} else {
		go c.doWrite(b)
	}

	return len(b), nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.closed {
		close(c.closer)
		c.closed = true
		return c.Conn.Close()
	}

	return nil
}

func (c *conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return c.Conn.SetDeadline(t)
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeDeadline = t
	return c.Conn.SetWriteDeadline(t)
}

func (c *conn) backgroundSimIntermediateReader() {
	for {
		select {
		case <-c.closer:
			return
		default:
			b := make([]byte, c.mtuSize)
			n, err := c.Conn.Read(b)
			if err != nil {
				return
			}

			c.intermediateDownQueue <- b[:n]
		}
	}
}

func (c *conn) backgroundSimReader() {
	fireAtAbsoluteTime := c.simDownInitialTime

	orderedAbsoluteReadTimes := c.simDownOrdered
	packetsToReadAtAbsTime := c.simDown

	for {
		var previousTimeMs int = 0
		for i, absoluteTimeMs := range orderedAbsoluteReadTimes {
			packetsToRead := packetsToReadAtAbsTime[absoluteTimeMs]

			var relativeTimeMs int = 0
			if i != 0 {
				relativeTimeMs = absoluteTimeMs - previousTimeMs
			}

			relativeTimeNanos := relativeTimeMs * 1e6
			fireAtAbsoluteTime = fireAtAbsoluteTime.Add(time.Duration(relativeTimeNanos))
			relativeCorrectedFireDuration := fireAtAbsoluteTime.Sub(time.Now())
			if relativeCorrectedFireDuration < 0 {
				/*if i != 0 {
					// CPU Constrained! We can't make realtime adjustments quick enough to keep up...
				}*/

				fireAtAbsoluteTime = time.Now()
				relativeCorrectedFireDuration = 0 * time.Millisecond
			}

			select {
			case <-c.closer:
				return
			case <-time.After(relativeCorrectedFireDuration):
				c.doSimRead(packetsToRead)
			}

			previousTimeMs = absoluteTimeMs
		}
	}
}

func (c *conn) doSimRead(packetsToRead int) {
	if packetsToRead == 0 {
		return
	}

	bytesAvailableToRead := packetsToRead * 1500

	// Pull these many bytes
	for bytesAvailableToRead > 0 {
		select {
		case <-c.closer:
			return
		case readPacket := <-c.intermediateDownQueue:
			if c.rand.Float64() >= c.packetLossRate {
				go func() {
					time.Sleep(c.minLatency + time.Duration(float64(c.maxLatency-c.minLatency)*c.rand.Float64()))
					select {
					case c.downQueue <- readPacket:
					default:
						// read queue full, drop packet
					}
				}()
			}

			bytesAvailableToRead -= len(readPacket)
		default:
			// Nothing left in queue -> underflowing read bandwidth
			return
		}
	}
}

func (c *conn) backgroundReader() {
	for {
		select {
		case <-c.closer:
			return
		default:
			b := make([]byte, c.mtuSize)
			n, err := c.Conn.Read(b)
			if err != nil {
				return
			}

			c.downQueue <- b[:n]
		}
	}
}

func (c *conn) backgroundSimWriter() {
	fireAtAbsoluteTime := c.simUpInitialTime

	orderedAbsoluteWriteTimes := c.simUpOrdered
	packetsToWriteAtAbsTime := c.simUp

	for {
		var previousTimeMs int = 0
		for i, absoluteTimeMs := range orderedAbsoluteWriteTimes {
			packetsToWrite := packetsToWriteAtAbsTime[absoluteTimeMs]

			var relativeTimeMs int = 0
			if i != 0 {
				relativeTimeMs = absoluteTimeMs - previousTimeMs
			}

			relativeTimeNanos := relativeTimeMs * 1e6
			fireAtAbsoluteTime = fireAtAbsoluteTime.Add(time.Duration(relativeTimeNanos))
			relativeCorrectedFireDuration := fireAtAbsoluteTime.Sub(time.Now())
			if relativeCorrectedFireDuration < 0 {
				/*if i != 0 {
					// CPU Constrained! We can't make realtime adjustments quick enough to keep up...
				}*/

				fireAtAbsoluteTime = time.Now()
				relativeCorrectedFireDuration = 0 * time.Millisecond
			}

			select {
			case <-c.closer:
				return
			case <-time.After(relativeCorrectedFireDuration):
				c.doSimWrite(packetsToWrite)
			}

			previousTimeMs = absoluteTimeMs
		}
	}
}

func (c *conn) doSimWrite(packetsToWrite int) {
	if packetsToWrite == 0 {
		return
	}

	bytesAvailableToWrite := packetsToWrite * 1500

	// Write up to these many bytes
	for bytesAvailableToWrite > 0 {
		select {
		case <-c.closer:
			return
		case writePacket := <-c.upQueue:
			bytesAvailableToWrite -= len(writePacket)
			go c.doWrite(writePacket)
		default:
			// Nothing left in queue -> underflowing write bandwidth
			return
		}
	}
}

func (c *conn) doWrite(b []byte) {
	if c.timeToWaitPerByte > 0 {
		c.throttleMu.Lock()
		time.Sleep(time.Duration(c.timeToWaitPerByte * (float64(len(b) + c.headerOverhead))))
		c.throttleMu.Unlock()
	}

	if c.rand.Float64() >= c.packetLossRate {
		time.Sleep(c.minLatency + time.Duration(float64(c.maxLatency-c.minLatency)*c.rand.Float64()))
		c.mu.Lock()
		_, _ = c.Conn.Write(b)
		c.mu.Unlock()
	}
}
