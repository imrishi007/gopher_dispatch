package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"
)

func main() {
	name := flag.String("name", "service", "service name")
	port := flag.Int("port", 8081, "port")
	flag.Parse()
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"service": *name, "status": "ok", "time": time.Now()})
	})
	fmt.Printf("%s on :%d\n", *name, *port)
	_ = http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
}
