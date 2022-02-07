package okexapi

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const ENDPOINT = "https://www.okex.com"
const AWSENDPOINT = "https://aws.okex.com"

// Please do not send more than 10 requests per second. Sending requests more frequently will result in HTTP 429 errors.
type Client struct {
	key, secret string
	subaccount  string
	passphrase  string
	aws         bool
	HTTPC       *http.Client
}

func New(key, secret, passphrase, subaccount string, aws bool) *Client {
	hc := &http.Client{
		Timeout: 10 * time.Second,
	}
	return &Client{
		key:        key,
		secret:     secret,
		passphrase: passphrase,
		subaccount: subaccount,
		HTTPC:      hc,
		aws:        aws,
	}
}

func (p *Client) newRequest(method, spath string, body []byte, params *map[string]string) (*http.Request, error) {
	var endpoint string
	switch p.aws {
	case true:
		endpoint = AWSENDPOINT
	default:
		endpoint = endpoint
	}
	u, _ := url.ParseRequestURI(endpoint)
	u.Path = u.Path + spath
	if params != nil {
		q := u.Query()
		for k, v := range *params {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	timestamp := IsoTime()
	preHash := PreHashString(timestamp, method, spath, string(body))
	sign, err := HmacSha256Base64Signer(preHash, p.secret)
	if err != nil {
		return nil, err
	}
	p.Headers(req, timestamp, sign)
	return req, nil
}

func MakeHMAC(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *Client) sendRequest(method, spath string, body []byte, params *map[string]string) (*http.Response, error) {
	req, err := c.newRequest(method, spath, body, params)
	if err != nil {
		return nil, err
	}
	res, err := c.HTTPC.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		//c.Logger.Printf("status: %s", res.Status)
		buf := new(bytes.Buffer)
		buf.ReadFrom(res.Body)
		return nil, fmt.Errorf("faild to get data. status: %s", res.Status)
	}
	return res, nil
}

func decode(res *http.Response, out interface{}) error {
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	err := json.Unmarshal([]byte(body), &out)
	if err == nil {
		return nil
	}
	return err
}

func responseLog(res *http.Response) string {
	b, _ := httputil.DumpResponse(res, true)
	return string(b)
}
func requestLog(req *http.Request) string {
	b, _ := httputil.DumpRequest(req, true)
	return string(b)
}

func IsoTime() string {
	utcTime := time.Now().UTC()
	iso := utcTime.String()
	isoBytes := []byte(iso)
	iso = string(isoBytes[:10]) + "T" + string(isoBytes[11:23]) + "Z"
	return iso
}

func PreHashString(timestamp string, method string, requestPath string, body string) string {
	return timestamp + strings.ToUpper(method) + requestPath + body
}

func HmacSha256Base64Signer(message string, secretKey string) (string, error) {
	mac := hmac.New(sha256.New, []byte(secretKey))
	_, err := mac.Write([]byte(message))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (c *Client) Headers(request *http.Request, timestamp string, sign string) {
	request.Header.Add("Accept", "application/json")
	request.Header.Add("Content-Type", "application/json; charset=UTF-8")
	request.Header.Add("OK-ACCESS-KEY", c.key)
	request.Header.Add("OK-ACCESS-SIGN", sign)
	request.Header.Add("OK-ACCESS-TIMESTAMP", timestamp)
	request.Header.Add("OK-ACCESS-PASSPHRASE", c.passphrase)
}

func (c *Client) SocketEndPointHub(private bool) (endpoint string) {
	switch private {
	case true:
		switch c.aws {
		case true:
			endpoint = "wss://wspap.okex.com:8443/ws/v5/private?brokerId=9999"
		default:
			endpoint = "wss://ws.okex.com:8443/ws/v5/private"
		}
	default:
		// didn't see aws entry on lastest version so far
		switch c.aws {
		case true:
			endpoint = "wss://real.okex.com:8443/ws/v3"
		default:
			endpoint = "wss://real.okex.com:8443/ws/v3"
		}
	}
	return endpoint
}
