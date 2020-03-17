package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

var conn net.Conn

type roi struct {
	Times      float64 `json:"times"`
	Currency   string  `json:"currency"`
	Percentage float64 `json:"percentage"`
}

type sparkline struct {
	Price []float64 `json:"price"`
}

type coin struct {
	ID                                 string    `json:"id"`
	Symbol                             string    `json:"symbol"`
	Name                               string    `json:"name"`
	Image                              string    `json:"image"`
	CurrentPrice                       float64   `json:"current_price"`
	MarketCap                          float64   `json:"market_cap"`
	MarketCapRank                      int16     `json:"market_cap_rank"`
	TotalVolume                        float64   `json:"total_volume"`
	High24                             float64   `json:"high_24h"`
	Low24                              float64   `json:"low_24h"`
	PriceChange24h                     float64   `json:"price_change_24h"`
	PriceChangePercentage24h           float64   `json:"price_change_percentage_24h"`
	MarketCapChange24h                 float64   `json:"market_cap_change_24h"`
	MarketCapChangePercentage24h       float64   `json:"market_cap_change_percentage_24h"`
	CirculatingSupply                  float64   `json:"circulating_supply"`
	TotalSupply                        float64   `json:"total_supply"`
	ATH                                float64   `json:"ath"`
	ATHChangePercentage                float64   `json:"ath_change_percentage"`
	ATHDate                            string    `json:"ath_date"`
	ROI                                roi       `json:"roi"`
	LastUpdated                        string    `json:"last_updated"`
	SparklineIn7d                      sparkline `json:"sparkline_in_7d"`
	PriceChangePercentage1hInCurrency  float64   `json:"price_change_percentage_1h_in_currency"`
	PriceChangePercentage24hInCurrency float64   `json:"price_change_percentage_24h_in_currency"`
	PriceChangePercentage7dInCurrency  float64   `json:"price_change_percentage_7d_in_currency"`
	PriceChangePercentage30dInCurrency float64   `json:"price_change_percentage_30d_in_currency"`
	PriceChangePercentage1yInCurrency  float64   `json:"price_change_percentage_1y_in_currency"`
}

func dial(netw, addr string) (net.Conn, error) {
	if conn != nil {
		conn.Close()
		conn = nil
	}

	netc, err := net.DialTimeout(netw, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	conn = netc

	return netc, nil
}

var reader io.ReadCloser

func closeConn() {
	if conn != nil {
		conn.Close()
	}
	if reader != nil {
		reader.Close()
	}
}

var (
	setupOnce  sync.Once
	httpClient *http.Client
)

func makeRequest(req *http.Request) (*http.Response, error) {
	setupOnce.Do(func() {
		httpClient = &http.Client{
			Transport: &http.Transport{
				Dial: dial,
			},
		}
	})
	return httpClient.Do(req)
}

func encodeCoinData(coinData coin) bytes.Buffer {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(coinData); err != nil {
		log.Println("Error encoding coinData:", err)
	}
	return b
}
