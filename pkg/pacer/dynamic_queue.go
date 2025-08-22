// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pacer

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/gammazero/deque"
	"github.com/livekit/protocol/logger"
)

type queuedPacket struct {
	p        *Packet
	enqueued time.Time
}

type DynamicQueue struct {
	*Base
	logger              logger.Logger
	lock                sync.RWMutex
	packets             deque.Deque[*queuedPacket]
	wake                chan struct{}
	isStopped           bool
	maxQueueSize        int
	deadlineMs          int64
	totalBytesSent      int64
	totalPacketsSent    int64
	lastReportTime      time.Time
	estimatedIntervalMs float64
}

func NewDynamicQueue(logger logger.Logger, maxQueueSize int, deadlineMs int64) *DynamicQueue {
	d := &DynamicQueue{
		Base:                NewBase(logger),
		logger:              logger,
		wake:                make(chan struct{}, 1),
		maxQueueSize:        maxQueueSize,
		deadlineMs:          deadlineMs,
		lastReportTime:      time.Now(),
		estimatedIntervalMs: 1.0, // Initial estimate
	}
	d.packets.SetBaseCap(512)
	return d
}

func (d *DynamicQueue) Start() {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.isStopped {
		return
	}
	d.lastReportTime = time.Now()
	go d.sendWorker()
	go d.reportWorker()
}

func (d *DynamicQueue) Stop() {
	d.lock.Lock()
	if d.isStopped {
		d.lock.Unlock()
		return
	}
	close(d.wake)
	d.isStopped = true
	d.lock.Unlock()
}

func (d *DynamicQueue) Enqueue(p *Packet) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.packets.Len() >= d.maxQueueSize {
		slog.Default().Error("packet dropped", "reason", "queue full")
		return
	}
	qp := &queuedPacket{p: p, enqueued: time.Now()}
	d.packets.PushBack(qp)
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *DynamicQueue) sendWorker() {
	var timer *time.Timer
	for {
		d.lock.RLock()
		if d.isStopped {
			d.lock.RUnlock()
			return
		}
		if d.packets.Len() == 0 {
			d.lock.RUnlock()
			_, ok := <-d.wake
			if !ok {
				return
			}
			continue
		}
		d.lock.RUnlock()

		defaultWait := time.Duration(math.Max(1, d.estimatedIntervalMs)) * time.Millisecond

		var wait time.Duration = defaultWait

		if timer != nil {
			timer.Stop()
		}
		if wait > 0 {
			timer = time.NewTimer(wait)
			select {
			case <-timer.C:
				// proceed to send
			case _, ok := <-d.wake:
				if !ok {
					return
				}
				continue
			}
		}

		// Send the packet
		d.lock.Lock()
		if d.packets.Len() == 0 {
			d.lock.Unlock()
			continue
		}
		qp := d.packets.PopFront()
		d.lock.Unlock()

		now := time.Now()
		deadlineDur := time.Duration(d.deadlineMs) * time.Millisecond
		deadlineTime := qp.enqueued.Add(deadlineDur)
		if deadlineTime.Before(now) || deadlineTime.Equal(now) {
			slog.Default().Error("packet dropped", "reason", "deadline exceeded")
			continue
		}

		size := qp.p.Header.MarshalSize() + len(qp.p.Payload)
		d.Base.SendPacket(qp.p)

		d.lock.Lock()
		d.totalBytesSent += int64(size)
		d.totalPacketsSent++
		d.lock.Unlock()
	}
}

func (d *DynamicQueue) reportWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		d.lock.RLock()
		if d.isStopped {
			d.lock.RUnlock()
			return
		}
		pressure := float64(d.packets.Len()) / float64(d.maxQueueSize) * 100
		now := time.Now()
		dur := now.Sub(d.lastReportTime).Seconds()
		var bitrate float64
		if dur > 0 {
			bitrate = float64(d.totalBytesSent*8) / dur
		}
		d.lock.RUnlock()

		slog.Default().Info("pacer report", "pressure_percent", fmt.Sprintf("%.2f", pressure), "estimated_bitrate_bps", fmt.Sprintf("%.2f", bitrate))

		d.lock.Lock()
		if d.totalPacketsSent > 0 {
			averagePacketSize := float64(d.totalBytesSent) / float64(d.totalPacketsSent)
			if averagePacketSize > 0 {
				packetRate := bitrate / averagePacketSize
				if packetRate > 0 {
					d.estimatedIntervalMs = 1000 / packetRate
				}
			}
		}
		d.totalBytesSent = 0
		d.totalPacketsSent = 0
		d.lastReportTime = now
		d.lock.Unlock()
	}
}

// ------------------------------------------------
