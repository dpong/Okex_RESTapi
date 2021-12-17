package okexapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

type OrderBookBranch struct {
	Bids          BookBranch
	Asks          BookBranch
	LastUpdatedId decimal.Decimal
	SnapShoted    bool
	Cancel        *context.CancelFunc
}

type BookBranch struct {
	mux  sync.RWMutex
	Book [][]string
}

func (o *OrderBookBranch) UpdateNewComing(message *map[string]interface{}) {
	var wg sync.WaitGroup
	data := (*message)["data"].([]interface{})
	for _, item := range data {
		if book, ok := item.(map[string]interface{}); ok {
			if bids, ok := book["bids"].([]interface{}); ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, item := range bids {
						if levelData, ok := item.([]interface{}); ok {
							price, okPrice := levelData[0].(string)
							size, okSize := levelData[1].(string)
							if !okPrice || !okSize {
								continue
							}
							decPrice, _ := decimal.NewFromString(price)
							decSize, _ := decimal.NewFromString(size)
							o.DealWithBidPriceLevel(decPrice, decSize)
						}
					}
				}()
			}
			if asks, ok := book["asks"].([]interface{}); ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for _, item := range asks {
						if levelData, ok := item.([]interface{}); ok {
							price, okPrice := levelData[0].(string)
							size, okSize := levelData[1].(string)
							if !okPrice || !okSize {
								continue
							}
							decPrice, _ := decimal.NewFromString(price)
							decSize, _ := decimal.NewFromString(size)
							o.DealWithAskPriceLevel(decPrice, decSize)
						}
					}
				}()
			}
			wg.Wait()
		}
	}
}

func (o *OrderBookBranch) DealWithBidPriceLevel(price, qty decimal.Decimal) {
	o.Bids.mux.Lock()
	defer o.Bids.mux.Unlock()
	l := len(o.Bids.Book)
	for level, item := range o.Bids.Book {
		bookPrice, _ := decimal.NewFromString(item[0])
		switch {
		case price.GreaterThan(bookPrice):
			// insert level
			if qty.IsZero() {
				// ignore
				return
			}
			o.Bids.Book = append(o.Bids.Book, []string{})
			copy(o.Bids.Book[level+1:], o.Bids.Book[level:])
			o.Bids.Book[level] = []string{price.String(), qty.String()}
			return
		case price.LessThan(bookPrice):
			if level == l-1 {
				// insert last level
				if qty.IsZero() {
					// ignore
					return
				}
				o.Bids.Book = append(o.Bids.Book, []string{price.String(), qty.String()})
				return
			}
			continue
		case price.Equal(bookPrice):
			if qty.IsZero() {
				// delete level
				o.Bids.Book = append(o.Bids.Book[:level], o.Bids.Book[level+1:]...)
				return
			}
			o.Bids.Book[level][1] = qty.String()
			return
		}
	}
}

func (o *OrderBookBranch) DealWithAskPriceLevel(price, qty decimal.Decimal) {
	o.Asks.mux.Lock()
	defer o.Asks.mux.Unlock()
	l := len(o.Asks.Book)
	for level, item := range o.Asks.Book {
		bookPrice, _ := decimal.NewFromString(item[0])
		switch {
		case price.LessThan(bookPrice):
			// insert level
			if qty.IsZero() {
				// ignore
				return
			}
			o.Asks.Book = append(o.Asks.Book, []string{})
			copy(o.Asks.Book[level+1:], o.Asks.Book[level:])
			o.Asks.Book[level] = []string{price.String(), qty.String()}
			return
		case price.GreaterThan(bookPrice):
			if level == l-1 {
				// insert last level
				if qty.IsZero() {
					// ignore
					return
				}
				o.Asks.Book = append(o.Asks.Book, []string{price.String(), qty.String()})
				return
			}
			continue
		case price.Equal(bookPrice):
			if qty.IsZero() {
				// delete level
				o.Asks.Book = append(o.Asks.Book[:level], o.Asks.Book[level+1:]...)
				return
			}
			o.Asks.Book[level][1] = qty.String()
			return
		}
	}
}

func (o *OrderBookBranch) Close() {
	(*o.Cancel)()
	o.SnapShoted = true
	o.Bids.mux.Lock()
	o.Bids.Book = [][]string{}
	o.Bids.mux.Unlock()
	o.Asks.mux.Lock()
	o.Asks.Book = [][]string{}
	o.Asks.mux.Unlock()
}

// return bids, ready or not
func (o *OrderBookBranch) GetBids() ([][]string, bool) {
	o.Bids.mux.RLock()
	defer o.Bids.mux.RUnlock()
	if len(o.Bids.Book) == 0 || !o.SnapShoted {
		return [][]string{}, false
	}
	book := o.Bids.Book
	return book, true
}

func (o *OrderBookBranch) GetBidsEnoughForValue(value decimal.Decimal) ([][]string, bool) {
	o.Bids.mux.RLock()
	defer o.Bids.mux.RUnlock()
	if len(o.Bids.Book) == 0 || !o.SnapShoted {
		return [][]string{}, false
	}
	var loc int
	var sumValue decimal.Decimal
	for level, data := range o.Bids.Book {
		if len(data) != 2 {
			return [][]string{}, false
		}
		price, _ := decimal.NewFromString(data[0])
		size, _ := decimal.NewFromString(data[1])
		sumValue = sumValue.Add(price.Mul(size))
		if sumValue.GreaterThan(value) {
			loc = level
			break
		}
	}
	book := o.Bids.Book[:loc+1]
	return book, true
}

// return asks, ready or not
func (o *OrderBookBranch) GetAsks() ([][]string, bool) {
	o.Asks.mux.RLock()
	defer o.Asks.mux.RUnlock()
	if len(o.Asks.Book) == 0 || !o.SnapShoted {
		return [][]string{}, false
	}
	book := o.Asks.Book
	return book, true
}

func (o *OrderBookBranch) GetAsksEnoughForValue(value decimal.Decimal) ([][]string, bool) {
	o.Asks.mux.RLock()
	defer o.Asks.mux.RUnlock()
	if len(o.Asks.Book) == 0 || !o.SnapShoted {
		return [][]string{}, false
	}
	var loc int
	var sumValue decimal.Decimal
	for level, data := range o.Asks.Book {
		if len(data) != 2 {
			return [][]string{}, false
		}
		price, _ := decimal.NewFromString(data[0])
		size, _ := decimal.NewFromString(data[1])
		sumValue = sumValue.Add(price.Mul(size))
		if sumValue.GreaterThan(value) {
			loc = level
			break
		}
	}
	book := o.Asks.Book[:loc+1]
	return book, true
}

// symbol example: UST-USDT
func (c *Client) LocalOrderBook(symbol string, logger *log.Logger) *OrderBookBranch {
	var o OrderBookBranch
	ctx, cancel := context.WithCancel(context.Background())
	o.Cancel = &cancel
	bookticker := make(chan map[string]interface{}, 50)
	errCh := make(chan error, 5)
	refreshCh := make(chan error, 5)
	symbol = strings.ToUpper(symbol)
	url := c.SocketEndPointHub(false)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := OkexOrderBookSocket(ctx, url, symbol, "orderbook", logger, &bookticker, &refreshCh); err == nil {
					return
				} else {
					// test
					fmt.Println(err.Error())
				}
				errCh <- errors.New("Reconnect websocket")
				time.Sleep(time.Second)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				o.MaintainOrderBook(ctx, symbol, &bookticker, &errCh, &refreshCh)
				logger.Warningf("Refreshing %s  local orderbook.\n", symbol)
				time.Sleep(time.Second)
			}
		}
	}()
	return &o
}

func (o *OrderBookBranch) MaintainOrderBook(
	ctx context.Context,
	symbol string,
	bookticker *chan map[string]interface{},
	errCh *chan error,
	refreshCh *chan error,
) {
	var storage []map[string]interface{}
	o.SnapShoted = false
	o.LastUpdatedId = decimal.NewFromInt(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-(*errCh):
			return
		default:
			message := <-(*bookticker)
			if len(message) != 0 {
				// spot
				// for initial orderbook
				if action, ok := message["action"].(string); ok {
					switch action {
					case "snapshot":
						o.InitialOrderBook(&message)
						continue
					case "update":
						if !o.SnapShoted {
							storage = append(storage, message)
							continue
						}
						if len(storage) > 1 {
							for _, data := range storage {
								if err := o.SpotUpdateJudge(&data); err != nil {
									*refreshCh <- err
								}
							}
							// clear storage
							storage = make([]map[string]interface{}, 0)
						}
						// handle incoming data
						if err := o.SpotUpdateJudge(&message); err != nil {
							*refreshCh <- err
						}
					}
				}
			}
		}
	}
}

func (o *OrderBookBranch) SpotUpdateJudge(message *map[string]interface{}) error {
	if data, ok := (*message)["data"].([]interface{}); ok {
		o.UpdateNewComing(message)
		for _, item := range data {
			if book, ok := item.(map[string]interface{}); ok {
				if checkSum, ok := book["checksum"].(float64); ok {
					if err := o.CheckCheckSum(uint32(checkSum)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (o *OrderBookBranch) InitialOrderBook(res *map[string]interface{}) {
	var wg sync.WaitGroup
	data := (*res)["data"].([]interface{})
	for _, item := range data {
		if book, ok := item.(map[string]interface{}); ok {
			if bids, ok := book["bids"].([]interface{}); ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					o.Bids.mux.Lock()
					defer o.Bids.mux.Unlock()
					o.Bids.Book = [][]string{}
					for _, item := range bids {
						if levelData, ok := item.([]interface{}); ok {
							price, okPrice := levelData[0].(string)
							size, okSize := levelData[1].(string)
							if !okPrice || !okSize {
								continue
							}
							o.Bids.Book = append(o.Bids.Book, []string{price, size})
						}
					}
				}()
			}
			if asks, ok := book["asks"].([]interface{}); ok {
				wg.Add(1)
				go func() {
					defer wg.Done()
					o.Asks.mux.Lock()
					defer o.Asks.mux.Unlock()
					o.Asks.Book = [][]string{}
					for _, item := range asks {
						if levelData, ok := item.([]interface{}); ok {
							price, okPrice := levelData[0].(string)
							size, okSize := levelData[1].(string)
							if !okPrice || !okSize {
								continue
							}
							o.Asks.Book = append(o.Asks.Book, []string{price, size})
						}
					}
				}()
			}
			wg.Wait()
		}
	}
	//o.LastUpdatedId = id
	o.SnapShoted = true
}

type OkexWebsocket struct {
	Channel       string
	OnErr         bool
	Logger        *log.Logger
	Conn          *websocket.Conn
	LastUpdatedId decimal.Decimal
}

type OkexSubscribeMessage struct {
	Op   string          `json:"op"`
	Args []OkexPublicArg `json:"args,omitempty"`
}

type OkexPublicArg struct {
	Channel string `json:"channel"`
	Instid  string `json:"instId"`
}

func (w *OkexWebsocket) OutOkexErr() map[string]interface{} {
	w.OnErr = true
	m := make(map[string]interface{})
	return m
}

func DecodingMap(message []byte, logger *log.Logger) (res map[string]interface{}, err error) {
	if message == nil {
		err = errors.New("the incoming message is nil")
		return nil, err
	}
	err = json.Unmarshal(message, &res)
	if err != nil {
		if try := Bytes2String(message); try != "pong" {
			return nil, err
		}
	}
	return res, nil
}

func OkexOrderBookSocket(
	ctx context.Context,
	url, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
	refreshCh *chan error,
) error {
	var w OkexWebsocket
	var duration time.Duration = 30
	w.Logger = logger
	w.OnErr = false
	innerErr := make(chan error, 1)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	logger.Infof("Okex %s orderBook socket connected.\n", symbol)
	w.Conn = conn
	defer conn.Close()
	send := GetOkexSubscribeMessage(channel, symbol)
	if err := w.Conn.WriteMessage(websocket.TextMessage, send); err != nil {
		return err
	}
	if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
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
				send := w.GetPingPong()
				if err := w.Conn.WriteMessage(websocket.TextMessage, send); err != nil {
					w.Conn.SetReadDeadline(time.Now().Add(time.Second))
					return
				}
				w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration))
			default:
				time.Sleep(time.Second)
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-*refreshCh:
			return err
		default:
			if conn == nil {
				d := w.OutOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			_, buf, err := conn.ReadMessage()
			if err != nil {
				d := w.OutOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			res, err1 := DecodingMap(buf, logger)
			if err1 != nil {
				d := w.OutOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message, err1)
				innerErr <- errors.New("restart")
				return err1
			}
			err2 := w.HandleOkexSocketData(&res, mainCh)
			if err2 != nil {
				d := w.OutOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message, err2)
				innerErr <- errors.New("restart")
				return err2
			}
			if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				return err
			}
		}
	}
}

func (w *OkexWebsocket) HandleOkexSocketData(res *map[string]interface{}, mainCh *chan map[string]interface{}) error {

	if event, ok := (*res)["event"].(string); ok {
		switch event {
		case "subscribe":
			if data, ok := (*res)["arg"].(map[string]interface{}); ok {
				channel := data["channel"].(string)
				inst := data["instId"].(string)
				w.Logger.Infof("Subscribed %s %s", inst, channel)
				return nil
			}
		}
	} else {
		if action, ok := (*res)["action"].(string); ok {
			switch action {
			case "snapshot":
				*mainCh <- *res
				return nil
			case "update":
				*mainCh <- *res
				return nil
			}
		}
	}
	return nil
}

func GetOkexSubscribeMessage(channel, symbol string) (message []byte) {
	switch channel {
	case "orderbook":
		arg := OkexPublicArg{
			Channel: "books50-l2-tbt",
			Instid:  symbol,
		}
		sub := OkexSubscribeMessage{
			Op:   "subscribe",
			Args: []OkexPublicArg{arg},
		}
		by, err := json.Marshal(sub)
		if err != nil {
			return nil
		}
		message = by
	}
	return message
}

func (t *OkexWebsocket) GetPingPong() []byte {
	sub := "ping"
	return String2Bytes(sub)
}

func String2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (o *OrderBookBranch) CheckCheckSum(checkSum uint32) error {
	var buffer bytes.Buffer
	o.Bids.mux.RLock()
	o.Asks.mux.RLock()
	defer o.Bids.mux.RUnlock()
	defer o.Asks.mux.RUnlock()
	if len(o.Bids.Book) == 0 || len(o.Asks.Book) == 0 {
		return nil
	}
	var level int = 25
	for i := 0; i < level; i++ {
		buffer.WriteString(o.Bids.Book[i][0])
		buffer.WriteString(":")
		buffer.WriteString(o.Bids.Book[i][1])
		buffer.WriteString(":")
		buffer.WriteString(o.Asks.Book[i][0])
		buffer.WriteString(":")
		buffer.WriteString(o.Asks.Book[i][1])
		if i != level-1 {
			buffer.WriteString(":")
		}
	}
	localCheckSum := crc32.ChecksumIEEE(buffer.Bytes())
	if localCheckSum != checkSum {
		return errors.New("checkSum error")
	}
	return nil
}
