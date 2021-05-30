package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/teramont/go2-lab-2/httptools"
	"github.com/teramont/go2-lab-2/signal"
)

var port = flag.Int("port", 8080, "server port")
var dbPort = flag.Int("db", 8070, "db port")

const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	dbAddr := fmt.Sprintf("http://db:%d", *dbPort)
	today := time.Now().Format("02-01-2006")
	url := fmt.Sprintf("%s/db/%s", dbAddr, "zbs-team")
	body, _ := json.Marshal(struct {
		Value string `json:"value"`
	}{
		Value: today,
	})
	req, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	if req.StatusCode != http.StatusOK {
		log.Fatalf("Database error: status code = %s", req.Status)
	}

	r := mux.NewRouter()

	r.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	r.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.FormValue("key")
		if key == "" {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		url := fmt.Sprintf("%s/db/%s", dbAddr, key)
		resp, err := http.Get(url)

		if err != nil {
			log.Printf("Failed to get response from: %s", err)
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		_, err = io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
			rw.WriteHeader(http.StatusInternalServerError)
		} else {
			rw.WriteHeader(resp.StatusCode)
		}
	})

	r.Handle("/report", report)

	h := new(http.ServeMux)

	h.Handle("/", r)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
