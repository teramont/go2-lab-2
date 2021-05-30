package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/teramont/go2-lab-2/cmd/datastore"
	"github.com/teramont/go2-lab-2/httptools"
	"github.com/teramont/go2-lab-2/signal"
)

const KB = 1024
const MB = KB * 1024

var port = flag.Int("p", 8070, "server's port")
var path = flag.String("d", "database", "database's directory path")
var segmentSize = flag.Int("s", 10*MB, "segment size in bytes")

func main() {
	flag.Parse()

	err := os.MkdirAll(*path, os.ModePerm)
	if err != nil {
		log.Fatalf("error creating directory: %s", err)
	}

	db, err := datastore.NewDb(*path)
	if err != nil {
		log.Fatalf("error creating db: %s", err)
	}
	db.SegmentSize(int64(*segmentSize))
	db.Start()

	defer db.Close()

	r := mux.NewRouter()
	r.HandleFunc("/db/{key}", func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]

		log.Printf("GET %s", r.URL)

		value, err := db.Get(key)

		rw.Header().Set("content-type", "application/json")

		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
		} else {

			rw.WriteHeader(http.StatusOK)

			res := struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}{key, value}
			err := json.NewEncoder(rw).Encode(&res)

			if err != nil {
				log.Printf("Error while serving request: %s", err)
			}
		}
	}).Methods("GET")

	r.HandleFunc("/db/{key}", func(rw http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		key := vars["key"]
		log.Printf("POST %s", r.URL)

		rw.Header().Set("content-type", "application/json")

		var body struct {
			Value string `json:"value"`
		}
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		err = db.Put(key, body.Value)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
		} else {
			rw.WriteHeader(http.StatusOK)
		}

	}).Methods("POST")

	h := new(http.ServeMux)
	h.Handle("/", r)
	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
