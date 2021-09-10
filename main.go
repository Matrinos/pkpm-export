package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/mainflux/senml"
	"golang.org/x/net/context"
)

const (
	subject  = "channels>"
	name     = "mainflux-stream"
	cursorID = "mycursor-5"
)

var measures = map[string]bool{
	"ActiveEnergyTotal": true,
}

var things = map[string]bool{
	"848e5c18-4784-4d37-be21-27f452a3b7c5": true,
	"f07c0a29-c4fa-4ebb-b413-b3de4046540c": true,
	"feaf731c-0103-46c3-a7e9-31f0777f81b7": true,
}

type Messages []*Message

type Message struct {
	UpdateTime  string  `json:"updateTime"`
	PointNumber string  `json:"pointNumber"`
	Value       float64 `json:"value"`
}

type MessagePayload struct {
	Array []string
}

type LoginRequest struct {
	LoginRequestDTO LoginRequestDTOStruct `json:"loginRequestDTO"`
}

type LoginRequestDTOStruct struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

func Login(user string, pass string) (string, error) {
	// / See func authHandler for an example auth handler that produces a token

	login := LoginRequest{
		LoginRequestDTO: LoginRequestDTOStruct{
			Name:     user,
			Password: pass,
		},
	}

	b, err := json.Marshal(login)

	if err != nil {
		return "", err
	}

	req, err := http.NewRequest(http.MethodPost, "http://121.37.166.100:2002/api/Account/login", bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		return "", err
	}

	client := &http.Client{}
	res, err := client.Do(req)

	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		return "", errors.New("unexpected status code")
		// fmt.Println("Unexpected status code", res.StatusCode)
	}

	// Read the token out of the response body
	buf := new(bytes.Buffer)
	io.Copy(buf, res.Body)
	res.Body.Close()
	tokenString := strings.TrimSpace(buf.String())

	// Declared an empty map interface
	var result = make(map[string]string)

	// Unmarshal or Decode the JSON to the interface.
	err = json.Unmarshal([]byte(tokenString), &result)

	if err != nil {
		return "", err
	}

	return result["access_token"], nil
}

var token = ""

func IsExpired(tokenString string) bool {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		log.Fatalf(err.Error())
		return false
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		log.Fatalf("Can't convert token's claims to standard claims")
		return false
	}

	return time.Now().After(time.UnixMicro(int64(claims["exp"].(float64))))

}

func pushData(messages []Message) error {

	var payload []Message

	// for _, msg := range messages {
	// fmt.Println(msg.PointNumber)
	// b, err := json.Marshal(msg)
	// if err != nil {
	// 	fmt.Printf("Error: %s", err)
	// 	return err
	// }
	// fmt.Println(string(b))
	payload = append(payload, messages...)
	// }

	if len(payload) == 0 {
		return nil
	}

	p, err := json.Marshal(payload)
	fmt.Println("Payload:", string(p))
	if err != nil {
		fmt.Printf("Error: %s", err)
		return err
	}

	// print(string(p))
	req, err := http.NewRequest(http.MethodPost,
		"http://121.37.166.100:2002/api/Record/insertRecords?buildingId=34",
		bytes.NewBuffer(p))

	req.Header.Set("Content-Type", "application/json")

	if err != nil {
		log.Fatal(err)
	}

	if token == "" || IsExpired(token) {
		token, err = Login("qidi", "qidi")
		if err != nil {
			panic(err)
		}
	}

	// fmt.Println(token)
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Errored when sending request to the server")
		return err
	}

	defer resp.Body.Close()
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("data uploaded", resp.Status)
	fmt.Println(string(responseBody))

	return nil
}
func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	defer client.Close()

	// Create a stream attached to the NATS subject
	if err := client.CreateStream(ctx, subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}

	// Publish a message to "foo". (Optional)
	// if _, err := client.Publish(context.Background(), name, []byte("hello")); err != nil {
	// 	panic(err)
	// }

	// Reset cursor (Optinal)
	// if err := client.SetCursor(ctx, cursorID, name, 0, 0); err != nil {
	// 	fmt.Printf("Reset cursor error %v", err)
	// 	panic(err)
	// }

	startSubsciption(ctx, client)

}

func startSubsciption(ctx context.Context, client lift.Client) {
	// Retrieve offset to resume processing from.
	// offset, err := client.FetchCursor(ctx, cursorID, name, 0)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("Start offset is %d\n", offset)
	// var token = ""

	var dataList []Message

	if err := client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		if err != nil {
			fmt.Printf("Message error %v", err)
			panic(err)
		}

		thg := string(msg.Value()[40:76])

		if _, ok := things[thg]; !ok {
			return
		}
		// fmt.Println("thing", string(msg.Value()[39:76]))
		// fmt.Println("thing", string(msg.Value()[0:37]))
		raw, err := senml.Decode(msg.Value()[85:len(msg.Value())-10], senml.JSON)

		if err != nil {
			panic(err)
		}

		normalized, err := senml.Normalize(raw)
		if err != nil {
			panic(err)
		}

		// msgs := make([]{}}, len(normalized.Records))
		for _, v := range normalized.Records {
			// _, f := math.Modf(v.Time)

			// fmt.Printf("time %v", time.Unix(int64(i), 0))
			timeString := time.UnixMilli(int64(v.Time * 1000)).Format("2006-01-02T15:04:05.999Z")
			// fmt.Printf("time %v", timeString)

			measure := strings.Split(v.Name, ":")[1]

			if _, ok := measures[measure]; !ok {
				continue
			}

			msg := Message{
				PointNumber: thg + measure,
				UpdateTime:  timeString,
				Value:       *v.Value,
			}

			dataList = append(dataList, msg)
		}

		if len(dataList) >= 2 {
			go pushData(dataList)

			dataList = nil
		}

		// percent := rand.Intn(100); percent > 90
		// if (msg.Offset()+1)%2 == 0 {
		// 	fmt.Println("Partition:", msg.Partition(), "Index:", msg.Offset(), "Failed!")
		// 	// Reset offset to failed one
		// 	if err := client.SetCursor(ctx, cursorID, name, msg.Partition(), msg.Offset()-1); err != nil {
		// 		fmt.Printf("SetCursor error %v", err)
		// 	}
		// 	time.Sleep(3 * time.Second)
		// 	startSubsciption(ctx, client)

		// 	// <-ctx.Done()
		// } else {
		// 	// fmt.Println(msg.Timestamp(), msg.Offset(), string(msg.Key()), string(msg.Value()))
		// 	fmt.Println("Partition:", msg.Partition(), "Index:", msg.Offset(), "Data Received...")
		// 	// Checkpoint current position.
		// 	if err := client.SetCursor(ctx, cursorID, name, msg.Partition(), msg.Offset()); err != nil {
		// 		fmt.Printf("SetCursor error %v", err)
		// 		panic(err)
		// 	}
		// }

	}, lift.StartAtLatestReceived()); err != nil {
		fmt.Printf("Subscribe error %v", err)
		panic(err)
	}
	fmt.Println("Subscription started...")
	<-ctx.Done()
}
