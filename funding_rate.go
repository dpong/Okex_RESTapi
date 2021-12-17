package okexapi

import (
	"fmt"
	"net/http"
)

type FundingResponse struct {
	Future       string `json:"instrument_id"`
	Rate         string `json:"funding_rate"`
	RealizedRate string `json:"realized_rate"`
	InterestRate string `json:"interest_rate"`
	Time         string `json:"funding_time"`
}

func (p *Client) Fundings(symbol string) (futures []*FundingResponse) {
	body := fmt.Sprintf("/api/swap/v3/instruments/%s/historical_funding_rate", symbol)
	res, err := p.sendRequest(http.MethodGet, body, nil, nil)
	if err != nil {
		return nil
	}
	// in Close()
	err = decode(res, &futures)
	if err != nil {
		return nil
	}
	return futures
}
