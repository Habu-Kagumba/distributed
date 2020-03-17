package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	baseURL    = "https://api.coingecko.com/api/v3"
	vsCurrency = "usd"
)

var (
	client *mongo.Client
)

func connectDB(ctx context.Context) error {
	var err error
	log.Println("Connecting to mongodb")
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	return err
}

func closeDB(ctx context.Context) {
	client.Disconnect(ctx)
	log.Println("Disconnect mongodb connection")
}

type coinID struct {
	ID string
}

func loadIds(ctx context.Context) ([]string, error) {
	var ids []string

	collection := client.Database("crypto").Collection("coins")

	cur, err := collection.Find(ctx, bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var result coinID
		err := cur.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		ids = append(ids, result.ID)
	}
	return ids, cur.Err()
}

func getMarketData(ctx context.Context, coinData chan<- bytes.Buffer) {
	ids, err := loadIds(ctx)
	if err != nil {
		log.Println("Failed to load IDS:", err)
		return
	}

	params := make(url.Values)
	params.Add("ids", strings.Join(ids[:], ","))
	params.Add("vs_currency", "usd")
	params.Add("sparkline", "true")
	params.Add("price_percentage_change", "1h,24h,7d,30d,1y")

	url := fmt.Sprintf("%s/coins/markets?%s", baseURL, params.Encode())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Println("Creating Coin Market http.Request failed:", err)
		return
	}

	resp, err := makeRequest(req)
	if err != nil {
		log.Println("Error making request to API:", err)
		return
	}

	reader := resp.Body
	decoder := json.NewDecoder(reader)
	for {
		var coins []coin
		if err := decoder.Decode(&coins); err != nil {
			break
		}
		for _, id := range ids {
			for _, c := range coins {
				if c.ID == id {
					log.Println("Coin:", id)
					coinData <- encodeCoinData(c)
				}
			}
		}
	}
}

func startCoinDataStream(ctx context.Context, stopchan <-chan struct{}, coinData chan<- bytes.Buffer) <-chan struct{} {
	stoppedchan := make(chan struct{}, 1)
	go func() {
		defer func() {
			stoppedchan <- struct{}{}
		}()
		for {
			select {
			case <-stopchan:
				log.Println("Stopping CoinGecko...")
				return
			default:
				log.Println("Querying CoinGecko...")
				getMarketData(ctx, coinData)
				log.Println("  (waiting)")
				time.Sleep(10 * time.Second)
			}
		}
	}()
	return stoppedchan
}

func publishCoinData(coinData <-chan bytes.Buffer) <-chan struct{} {
	stopchan := make(chan struct{}, 1)
	pub, err := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	if err != nil {
		log.Println("NSQ producer creation failed:", err)
	}

	go func() {
		for cdata := range coinData {
			pub.Publish("coins", cdata.Bytes())
			log.Println(binary.Size(cdata.Bytes()))
			log.Println(len(cdata.Bytes()))
		}
		log.Println("Publisher: Stopping")
		pub.Stop()
		log.Println("Publisher: Stopped")
		stopchan <- struct{}{}
	}()

	return stopchan
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stoplock sync.Mutex
	stop := false
	stopChan := make(chan struct{}, 1)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		stoplock.Lock()
		stop = true
		log.Println("Stopping...")
		stopChan <- struct{}{}
		closeConn()
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	if err := connectDB(ctx); err != nil {
		log.Fatalln("Failed to connect to MongoDB")
	}
	log.Println("Successfully connected to MongoDB")
	defer closeDB(ctx)

	coinData := make(chan bytes.Buffer)
	publisherStoppedChan := publishCoinData(coinData)
	coinGeckoStoppedChan := startCoinDataStream(ctx, stopChan, coinData)
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			closeConn()
			stoplock.Lock()
			if stop {
				stoplock.Unlock()
				return
			}
			stoplock.Unlock()
		}
	}()
	<-coinGeckoStoppedChan
	close(coinData)
	<-publisherStoppedChan
}
