package server

import (
	"net/http"

	"github.com/gorilla/mux"
	//bioserver "github.com/xtao/bioflow/server"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

func NewRouter() *mux.Router {

	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		router.Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router
}

var routes = Routes{
	Route{
		"MountVolume",
		"POST",
		"/v1/auto/mount",
		MountVolume,
	},
	Route{
		"MountTest",
		"GET",
		"/v1/mountget",
		MountTest,
	},
	Route{
		"UMountVolume",
		"POST",
		"/v1/auto/umount",
		UMountVolume,
	},
}
