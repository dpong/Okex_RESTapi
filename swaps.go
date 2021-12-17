package okexapi

import (
	"fmt"
	"net/http"
	"time"
)

type SwapsResponse struct {
	InstrumentID        string `json:"instrument_id"`
	UnderlyingIndex     string `json:"underlying_index"`
	QuoteCurrency       string `json:"quote_currency"`
	Coin                string `json:"coin"`
	ContractVal         string `json:"contract_val"`
	Listing             string `json:"listing"`
	Delivery            string `json:"delivery"`
	SizeIncrement       string `json:"size_increment"`
	TickSize            string `json:"tick_size"`
	BaseCurrency        string `json:"base_currency"`
	Underlying          string `json:"underlying"`
	SettlementCurrency  string `json:"settlement_currency"`
	IsInverse           string `json:"is_inverse"`
	ContractValCurrency string `json:"contract_val_currency"`
}

func (p *Client) Swaps() (swaps []*SwapsResponse) {
	res, err := p.sendRequest(http.MethodGet, "/api/swap/v3/instruments", nil, nil)
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

func (p *Client) SwapsKLine(symbol string, resolution int, start, end time.Time) (swaps []interface{}) {
	params := make(map[string]string)
	params["granularity"] = fmt.Sprintf("%v", resolution)
	params["start"] = fmt.Sprintf("%v", start.UTC().Format("2006-01-02T15:04:05.999Z"))
	params["end"] = fmt.Sprintf("%v", end.UTC().Format("2006-01-02T15:04:05.999Z"))
	path := fmt.Sprintf("/api/swap/v3/instruments/%s/candles", symbol)
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
