package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8080", "control-plane URL")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		return
	}
	path, method := "", http.MethodGet
	switch args[0] {
	case "status":
		path = "/services"
	case "up":
		path, method = "/services/start", http.MethodPost
	case "start", "stop":
		if len(args) != 2 {
			usage()
			return
		}
		path, method = "/services/"+args[1]+"/"+args[0], http.MethodPost
	case "port":
		if len(args) != 2 {
			usage()
			return
		}
		path = "/ports/" + args[1]
	default:
		usage()
		return
	}
	req, _ := http.NewRequest(method, *addr+path, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	var pretty any
	if json.Unmarshal(b, &pretty) == nil {
		out, _ := json.MarshalIndent(pretty, "", "  ")
		fmt.Println(string(out))
		return
	}
	fmt.Println(string(b))
}
func usage() { fmt.Println("dispatch [status|up|start <service>|stop <service>|port <number>]") }
