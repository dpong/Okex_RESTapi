package okexapi

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"hash/crc32"
	"io/ioutil"
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
	reCh          chan error
	lastRefresh   lastRefreshBranch
}

type lastRefreshBranch struct {
	mux  sync.RWMutex
	time time.Time
}

type BookBranch struct {
	mux  sync.RWMutex
	Book [][]string
}

func (o *OrderBookBranch) IfCanRefresh() bool {
	o.lastRefresh.mux.Lock()
	defer o.lastRefresh.mux.Unlock()
	now := time.Now()
	if now.After(o.lastRefresh.time.Add(time.Second * 3)) {
		o.lastRefresh.time = now
		return true
	}
	return false
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

func (o *OrderBookBranch) RefreshLocalOrderBook(err error) error {
	if o.IfCanRefresh() {
		if len(o.reCh) == cap(o.reCh) {
			return errors.New("refresh channel is full, please check it up")
		}
		o.reCh <- err
	}
	return nil
}

func (o *OrderBookBranch) Close() {
	(*o.Cancel)()
	o.SnapShoted = false
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
	if !o.SnapShoted {
		return [][]string{}, false
	}
	if len(o.Bids.Book) == 0 {
		if o.IfCanRefresh() {
			o.reCh <- errors.New("re cause len bid is zero")
		}
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
	if !o.SnapShoted {
		return [][]string{}, false
	}
	if len(o.Asks.Book) == 0 {
		if o.IfCanRefresh() {
			o.reCh <- errors.New("re cause len ask is zero")
		}
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
	refreshCh := make(chan error, 5)
	o.reCh = make(chan error, 5)
	symbol = strings.ToUpper(symbol)
	url := c.SocketEndPointHub(false)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := okexOrderBookSocket(ctx, url, symbol, "orderbook", logger, &bookticker, &refreshCh); err == nil {
					return
				}
				// connection limit by okex
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
				err := o.maintainOrderBook(ctx, symbol, &bookticker)
				if err == nil {
					return
				}
				logger.Warningf("refreshing %s local orderbook cause: %s", symbol, err.Error())
				refreshCh <- errors.New("refreshing from maintain orderbook")
			}
		}
	}()
	return &o
}

func (o *OrderBookBranch) maintainOrderBook(
	ctx context.Context,
	symbol string,
	bookticker *chan map[string]interface{},
) error {
	//var storage []map[string]interface{}
	o.SnapShoted = false
	o.LastUpdatedId = decimal.NewFromInt(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-o.reCh:
			return err
		default:
			message := <-(*bookticker)
			if len(message) != 0 {
				// for initial orderbook
				if action, ok := message["action"].(string); ok {
					switch action {
					case "partial":
						o.initialOrderBook(&message)
						continue
					case "update":
						// handle incoming data
						if err := o.spotUpdateJudge(&message); err != nil {
							return err
						}
					}
				}
			}
		}
	}
}

func (o *OrderBookBranch) spotUpdateJudge(message *map[string]interface{}) error {
	if data, ok := (*message)["data"].([]interface{}); ok {
		o.UpdateNewComing(message)
		for _, item := range data {
			if book, ok := item.(map[string]interface{}); ok {
				if checkSum, ok := book["checksum"].(float64); ok {
					if err := o.checkCheckSum(uint32(checkSum)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (o *OrderBookBranch) initialOrderBook(res *map[string]interface{}) {
	var wg sync.WaitGroup
	data := (*res)["data"].([]interface{})
	for _, itemBig := range data {
		if book, ok := itemBig.(map[string]interface{}); ok {
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
	o.SnapShoted = true
}

type wS struct {
	Channel       string
	OnErr         bool
	Logger        *log.Logger
	Conn          *websocket.Conn
	LastUpdatedId decimal.Decimal
}

type OkexSubscribeMessage struct {
	Op   string   `json:"op"`
	Args []string `json:"args,omitempty"`
}

func (w *wS) outOkexErr() map[string]interface{} {
	w.OnErr = true
	m := make(map[string]interface{})
	return m
}

func decodingMap(message *[]byte, logger *log.Logger) (res map[string]interface{}, err error) {
	body := flate.NewReader(bytes.NewReader(*message))
	if err != nil {
		return nil, err
	}
	enflated, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(enflated, &res)
	if err != nil {
		if try := bytes2String(enflated); try != "pong" {
			return nil, err
		}
	}
	return res, nil
}

func okexOrderBookSocket(
	ctx context.Context,
	url, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
	refreshCh *chan error,
) error {
	var w wS
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
	send := getOkexSubscribeMessage(channel, symbol)
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
				send := w.getPingPong()
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
			innerErr <- errors.New("restart")
			return err
		default:
			if conn == nil {
				d := w.outOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			_, buf, err := conn.ReadMessage()
			if err != nil {
				d := w.outOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message)
				innerErr <- errors.New("restart")
				return errors.New(message)
			}
			res, err1 := decodingMap(&buf, logger)
			if err1 != nil {
				d := w.outOkexErr()
				*mainCh <- d
				message := "Okex reconnect..."
				logger.Infoln(message, err1)
				innerErr <- errors.New("restart")
				return err1
			}
			err2 := w.handleOkexSocketData(&res, mainCh)
			if err2 != nil {
				d := w.outOkexErr()
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

func (w *wS) handleOkexSocketData(res *map[string]interface{}, mainCh *chan map[string]interface{}) error {
	if event, ok := (*res)["event"].(string); ok {
		switch event {
		case "subscribe":
			if channel, ok := (*res)["channel"].(string); ok {
				w.Logger.Infof("Subscribed %s", channel)
				return nil
			}
		}
	} else {
		if table, ok := (*res)["table"].(string); ok {

			switch table {
			case "spot/ticker":
				if dataSet, ok := (*res)["data"].([]interface{}); !ok {
					m := w.outOkexErr()
					*mainCh <- m
					return errors.New("got nil when getting data for event time")
				} else {
					for _, item := range dataSet {
						data := item.(map[string]interface{})
						if st, ok := data["timestamp"].(string); !ok {
							m := w.outOkexErr()
							*mainCh <- m
							return errors.New("got nil when updating event time")
						} else {
							stamp, err := timeStringToDateTime(st)
							if err != nil {
								m := w.outOkexErr()
								*mainCh <- m
								return errors.New("fail to parse time when getting event time")
							}
							if time.Now().After(stamp.Add(time.Second * 5)) {
								m := w.outOkexErr()
								*mainCh <- m
								return errors.New("websocket data delay more than 5 sec")
							}
						}
					}
				}
				*mainCh <- *res
				return nil
			case "spot/depth_l2_tbt":
				if action, ok := (*res)["action"].(string); ok {
					if dataSet, ok := (*res)["data"].([]interface{}); !ok {
						m := w.outOkexErr()
						*mainCh <- m
						return errors.New("got nil when getting data for event time")
					} else {
						for _, item := range dataSet {
							data := item.(map[string]interface{})
							if st, ok := data["timestamp"].(string); !ok {
								m := w.outOkexErr()
								*mainCh <- m
								return errors.New("got nil when updating event time")
							} else {
								stamp, err := timeStringToDateTime(st)
								if err != nil {
									m := w.outOkexErr()
									*mainCh <- m
									return errors.New("fail to parse time when getting event time")
								}
								if time.Now().After(stamp.Add(time.Second * 5)) {
									m := w.outOkexErr()
									*mainCh <- m
									return errors.New("websocket data delay more than 5 sec")
								}
							}
						}
					}
					switch action {
					case "partial":
						*mainCh <- *res
						return nil
					case "update":
						*mainCh <- *res
						return nil
					}
				}
			}
		}
	}
	return nil
}

func getOkexSubscribeMessage(channel, symbol string) (message []byte) {
	switch channel {
	case "orderbook":
		var buffer bytes.Buffer
		buffer.WriteString("spot/depth_l2_tbt:")
		buffer.WriteString(symbol)
		arg := buffer.String()
		sub := OkexSubscribeMessage{
			Op:   "subscribe",
			Args: []string{arg},
		}
		by, err := json.Marshal(sub)
		if err != nil {
			return nil
		}
		message = by
	case "ticker":
		var buffer bytes.Buffer
		buffer.WriteString("spot/ticker:")
		buffer.WriteString(symbol)
		arg := buffer.String()
		sub := OkexSubscribeMessage{
			Op:   "subscribe",
			Args: []string{arg},
		}
		by, err := json.Marshal(sub)
		if err != nil {
			return nil
		}
		message = by
	}
	return message
}

func (t *wS) getPingPong() []byte {
	sub := "ping"
	return string2Bytes(sub)
}

func string2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
}

func bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (o *OrderBookBranch) checkCheckSum(checkSum uint32) error {
	o.Bids.mux.RLock()
	o.Asks.mux.RLock()
	defer o.Bids.mux.RUnlock()
	defer o.Asks.mux.RUnlock()
	if len(o.Bids.Book) == 0 || len(o.Asks.Book) == 0 {
		return nil
	}
	bidLen := len(o.Bids.Book)
	askLen := len(o.Asks.Book)
	var level int = 25
	var list []string
	for i := 0; i < level; i++ {
		if i < bidLen {
			list = append(list, o.Bids.Book[i][:2]...)
		}
		if i < askLen {
			list = append(list, o.Asks.Book[i][:2]...)
		}
	}
	result := strings.Join(list, ":")
	localCheckSum := crc32.ChecksumIEEE(string2Bytes(result))
	if localCheckSum != checkSum {
		return errors.New("checkSum error")
	}
	return nil
}

func timeStringToDateTime(input string) (time.Time, error) {
	layout := "2006-01-02T15:04:05.999Z"
	t, err := time.Parse(layout, input)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}
