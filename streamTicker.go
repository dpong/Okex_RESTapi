package okexapi

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/websocket"
)

const NullPrice = "null"

type StreamTickerBranch struct {
	bid    tobBranch
	ask    tobBranch
	cancel *context.CancelFunc
	reCh   chan error
	socket wS
}

type tobBranch struct {
	mux   sync.RWMutex
	price string
	qty   string
	ts    time.Time
}

// func SwapStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
// 	return localStreamTicker("swap", symbol, logger)
// }

func (c *Client) SpotStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
	return c.localStreamTicker("spot", symbol, logger)
}

func (c *Client) localStreamTicker(product, symbol string, logger *log.Logger) *StreamTickerBranch {
	var s StreamTickerBranch
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = &cancel
	ticker := make(chan map[string]interface{}, 50)
	errCh := make(chan error, 5)
	url := c.SocketEndPointHub(false)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := okexTickerSocket(ctx, url, symbol, "ticker", logger, &ticker); err == nil {
					return
				} else {
					logger.Warningf("Reconnect %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.maintainStreamTicker(ctx, product, symbol, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Refreshing %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	return &s
}

func (s *StreamTickerBranch) Close() {
	(*s.cancel)()
	s.bid.mux.Lock()
	s.bid.price = NullPrice
	s.bid.mux.Unlock()
	s.ask.mux.Lock()
	s.ask.price = NullPrice
	s.ask.mux.Unlock()
}

func (s *StreamTickerBranch) GetBid() (price, qty string, timeStamp time.Time, ok bool) {
	s.bid.mux.RLock()
	defer s.bid.mux.RUnlock()
	price = s.bid.price
	qty = s.bid.qty
	if price == NullPrice || price == "" {
		return price, qty, timeStamp, false
	}
	timeStamp = s.bid.ts
	return price, qty, timeStamp, true
}

func (s *StreamTickerBranch) GetAsk() (price, qty string, timeStamp time.Time, ok bool) {
	s.ask.mux.RLock()
	defer s.ask.mux.RUnlock()
	price = s.ask.price
	qty = s.ask.qty
	if price == NullPrice || price == "" {
		return price, qty, timeStamp, false
	}
	timeStamp = s.ask.ts
	return price, qty, timeStamp, true
}

func (s *StreamTickerBranch) updateBidData(price, qty string, timeStamp time.Time) {
	s.bid.mux.Lock()
	defer s.bid.mux.Unlock()
	s.bid.price = price
	s.bid.qty = qty
	s.bid.ts = timeStamp
}

func (s *StreamTickerBranch) updateAskData(price, qty string, timeStamp time.Time) {
	s.ask.mux.Lock()
	defer s.ask.mux.Unlock()
	s.ask.price = price
	s.ask.qty = qty
	s.ask.ts = timeStamp
}

func (s *StreamTickerBranch) maintainStreamTicker(
	ctx context.Context,
	product, symbol string,
	ticker *chan map[string]interface{},
	errCh *chan error,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-(*ticker):
			if dataSets, ok := message["data"].([]interface{}); !ok {
				continue
			} else {
				var bidPrice, askPrice, bidQty, askQty string
				var timeStamp time.Time
				for _, item := range dataSets {
					dataSet := item.(map[string]interface{})
					if asks, ok := dataSet["asks"].([]interface{}); ok {
						if len(asks) != 0 {
							tob := asks[0].([]interface{})
							if ask, ok := tob[0].(string); ok {
								askPrice = ask
							} else {
								askPrice = NullPrice
							}
							if askqty, ok := tob[1].(string); ok {
								askQty = askqty
							}
						}
					}
					if bids, ok := dataSet["bids"].([]interface{}); ok {
						if len(bids) != 0 {
							tob := bids[0].([]interface{})
							if bid, ok := tob[0].(string); ok {
								bidPrice = bid
							} else {
								bidPrice = NullPrice
							}
							if bidqty, ok := tob[1].(string); ok {
								bidQty = bidqty
							}
						}

					}
					if ts, ok := dataSet["ts"].(string); ok {
						tsInt, _ := strconv.ParseInt(ts, 10, 64)
						timeStamp = time.UnixMilli(tsInt)
					}

				}
				s.updateBidData(bidPrice, bidQty, timeStamp)
				s.updateAskData(askPrice, askQty, timeStamp)
			}
		}
	}
}

func okexTickerSocket(
	ctx context.Context,
	url, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
) error {
	var w wS
	var duration time.Duration = 30
	w.logger = logger
	innerErr := make(chan error, 1)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	logger.Infof("Okex %s ticker socket connected.\n", symbol)
	w.conn = conn
	defer conn.Close()
	send := getOkexSubscribeMessage(channel, symbol)
	if err := w.conn.WriteMessage(websocket.TextMessage, send); err != nil {
		return err
	}
	if err := w.conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
		return err
	}
	go func() {
		PingManaging := time.NewTicker(time.Second * 20)
		defer PingManaging.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-innerErr:
				return
			case <-PingManaging.C:
				send := w.getPingPong()
				if err := w.conn.WriteMessage(websocket.TextMessage, send); err != nil {
					w.conn.SetReadDeadline(time.Now().Add(time.Second))
					return
				}
				w.conn.SetReadDeadline(time.Now().Add(time.Second * duration))
			default:
				time.Sleep(time.Second)
			}
		}
	}()
	w.updateLastPongTime()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if w.conn == nil {
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			_, buf, err := w.conn.ReadMessage()
			if err != nil {
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			res, err1 := w.decodingMap(&buf, logger)
			if err1 != nil {
				message := "Okex reconnect..."
				logger.Infoln(message, err1)
				innerErr <- errors.New("restart")
				return err1
			}
			err2 := w.handleOkexSocketData(&res, mainCh)
			if err2 != nil {
				message := "Okex reconnect..."
				logger.Infoln(message, err2)
				innerErr <- errors.New("restart")
				return err2
			}
			if err := w.conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				return err
			}
		}
	}
}
