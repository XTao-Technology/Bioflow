.PHONY: all docker docker-clean publish-docker

REPO=github.com/xtao/xstone
#VERSION?=$(shell git describe HEAD | sed s/^v//)
VERSION?=1.0
DATE?=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
DOCKERNAME?=xtao/xstone
DOCKERTAG?=${DOCKERNAME}:${VERSION}
#LDFLAGS=-X ${REPO}/version.Version=${VERSION} -X ${REPO}/version.BuildDate=${DATE}
TOOLS=${GOPATH}/bin/go-bindata \
      ${GOPATH}/bin/go-bindata-assetfs \
      ${GOPATH}/bin/goconvey
SRC=$(shell find . -name '*.go')
TESTFLAGS="-v"

DOCKER_GO_SRC_PATH=/go/src/github.com/xtao/xstone
DOCKER_GOLANG_RUN_CMD=docker run --rm -v "$(PWD)":$(DOCKER_GO_SRC_PATH) -w $(DOCKER_GO_SRC_PATH) golang:1.6 bash -c

PACKAGES=$(shell go list ./... | grep -v /vendor/)


#${TOOLS}:
#	go get github.com/jteeuwen/go-bindata/...
#	go get github.com/elazarl/go-bindata-assetfs/...
#	go get github.com/smartystreets/goconvey


vet:
	go vet ${PACKAGES}

lint:
	go list ./... | grep -v /vendor/ | grep -v assets | xargs -L1 golint -set_exit_status


rpm/xstone/automount-manager-plugin: ${SRC}
	CGO_ENABLED=1 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/xstone/cmd/automount-manager

rpm: rpm/xstone/automount-manager-plugin
	cd rpm && ./autogen.sh
	cd rpm && ./configure
	cd rpm && make clean && make rpm

clean:
	rm -rf rpm/xstone/automount-manager-plugin

	rm -rf rpm/rpmbuild
	rm -rf rpm/config.*
	rm -rf rpm/configure
	rm -rf rpm/install.sh
	rm -rf rpm/Makefile
	rm -rf rpm/Makefile.in
	rm -rf rpm/*.tar.gz
	rm -rf rpm/automount-manager-plugin

publish-docker:
#ifeq ($(strip $(shell docker images --format="{{.Repository}}:{{.Tag}}" $(DOCKERTAG))),)
#	$(warning Docker tag does not exist:)
#	$(warning ${DOCKERTAG})
#	$(warning )
#	$(error Cannot publish the docker image. Please run `make docker` or `make docker-clean` first.)
#endif
	docker push ${DOCKERTAG}
	git describe HEAD --exact 2>/dev/null && \
		docker tag ${DOCKERTAG} ${DOCKERNAME}:latest && \
		docker push ${DOCKERNAME}:latest || true
