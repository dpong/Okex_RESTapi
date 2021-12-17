package okexapi

import (
	"fmt"
	"net/http"
	"time"
)

type FuturesResponse struct {
	InstrumentID        string `json:"instrument_id"`
	UnderlyingIndex     string `json:"underlying_index"`
	QuoteCurrency       string `json:"quote_currency"`
	TickSize            string `json:"tick_size"`
	ContractVal         string `json:"contract_val"`
	Listing             string `json:"listing"`
	Delivery            string `json:"delivery"`
	TradeIncrement      string `json:"trade_increment"`
	Alias               string `json:"alias"`
	Underlying          string `json:"underlying"`
	BaseCurrency        string `json:"base_currency"`
	SettlementCurrency  string `json:"settlement_currency"`
	IsInverse           string `json:"is_inverse"`
	ContractValCurrency string `json:"contract_val_currency"`
}

func (p *Client) Futures() (swaps []*FuturesResponse) {
	res, err := p.sendRequest(http.MethodGet, "/api/futures/v3/instruments", nil, nil)
	if err != nil {
		return nil
	}
	// in Close()
	err = decode(res, &swaps)
	if err != nil {
		return nil
	}
	return swaps
}

func (p *Client) FuturesKLine(symbol string, resolution int, start, end time.Time) (swaps []interface{}) {
	params := make(map[string]string)
	params["granularity"] = fmt.Sprintf("%v", resolution)
	params["start"] = fmt.Sprintf("%v", start.UTC().Format("2006-01-02T15:04:05.999Z"))
	params["end"] = fmt.Sprintf("%v", end.UTC().Format("2006-01-02T15:04:05.999Z"))
	path := fmt.Sprintf("/api/futures/v3/instruments/%s/candles", symbol)
	res, err := p.sendRequest(http.MethodGet, path, nil, &params)
	if err != nil {
		return nil
	}
	// in Close()
	err = decode(res, &swaps)
	if err != nil {
		return nil
	}
	return swaps
}
