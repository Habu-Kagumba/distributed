package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/bitly/go-nsq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

func decodeCoinData(encodedData bytes.Buffer) coin {
	var coinData coin
	d := gob.NewDecoder(&encodedData)
	if err := d.Decode(&coinData); err != nil {
		log.Println("Error decoding coinData:", err)
	}
	return coinData
}

var fatalErr error

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
	fatalErr = e
}

func saveMarketData(newCoinDataLock *sync.Mutex, newCoinData *map[string]coin, coinDataCollection *mongo.Collection) {

}

func main() {
	defer func() {
		if fatalErr != nil {
			os.Exit(1)
		}
	}()

	log.Println("Connecting to database...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		fatal(err)
		return
	}
	defer func() {
		log.Println("Closing database connection...")
		client.Disconnect(ctx)
	}()

	coinDataCollection := client.Database("crypto").Collection("coins")

	var newCoinData map[string]coin
	var newCoinDataLock sync.Mutex

	log.Println("Connecting to NSQ...")
	consumer, err := nsq.NewConsumer("coins", "market", nsq.NewConfig())
	if err != nil {
		fatal(err)
		return
	}

	consumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		newCoinDataLock.Lock()
		defer newCoinDataLock.Unlock()
		if newCoinData == nil {
			newCoinData = make(map[string]coin)
		}
		coinData := decodeCoinData(*bytes.NewBuffer(m.Body))
		newCoinData[coinData.ID] = coinData
		return nil
	}))

	if err := consumer.ConnectToNSQLookupd("localhost:4161"); err != nil {
		fatal(err)
		return
	}
}
