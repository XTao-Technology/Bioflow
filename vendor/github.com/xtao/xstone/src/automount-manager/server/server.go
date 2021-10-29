package server

import (
	"net/http"
)

type stoneRestServer struct {
	addr string
}

var GlobalRestServer *stoneRestServer = nil

func NewSTONERESTServer(addr string) *stoneRestServer {
	GlobalRestServer = &stoneRestServer{
		addr: addr,
	}
	return GlobalRestServer
}

func (server *stoneRestServer) StartRESTServer() {
	router := NewRouter()
	http.ListenAndServe(server.addr, router)
}
