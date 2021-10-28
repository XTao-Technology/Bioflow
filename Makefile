.PHONY: all docker docker-clean publish-docker

REPO=github.com/xtao/bioflow
#VERSION?=$(shell git describe HEAD | sed s/^v//)
VERSION?=1.1
DATE?=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
DOCKERNAME?=xtao/bioflow
DBDOCKERNAME?=xtao/bioflow-db
DOCKERTAG?=${DOCKERNAME}:${VERSION}
DBDOCKERTAG?=${DBDOCKERNAME}:${VERSION}
#LDFLAGS=-X ${REPO}/version.Version=${VERSION} -X ${REPO}/version.BuildDate=${DATE}
TOOLS=${GOPATH}/bin/go-bindata \
      ${GOPATH}/bin/go-bindata-assetfs \
      ${GOPATH}/bin/goconvey
SRC=$(shell find . -name '*.go')
TESTFLAGS="-v"

DOCKER_GO_SRC_PATH=/go/src/github.com/xtao/bioflow
DOCKER_GOLANG_RUN_CMD=docker run --rm -v "$(PWD)":$(DOCKER_GO_SRC_PATH) -w $(DOCKER_GO_SRC_PATH) golang:1.6 bash -c

PACKAGES=$(shell go list ./... | grep -v /vendor/)
BIOFLOW_VERSION=$(shell git rev-parse HEAD)


#${TOOLS}:
#	go get github.com/jteeuwen/go-bindata/...
#	go get github.com/elazarl/go-bindata-assetfs/...
#	go get github.com/smartystreets/goconvey


vet:
	go vet ${PACKAGES}

lint:
	go list ./... | grep -v /vendor/ | grep -v assets | xargs -L1 golint -set_exit_status


docker/bioflow/bioflow: ${SRC}
	CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/bioflow

docker/bioflow/biocli: ${SRC}
	CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/biocli

docker/bioflow/bioadm: ${SRC}
	CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/bioadm

docker/bioflow/xtfscli: ${SRC}
	CGO_ENABLED=0 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/xtfscli

docker: docker/bioflow/bioflow docker/bioflow/biocli docker/bioflow/bioadm docker/bioflow/Dockerfile docker/bioflow/marathon.sh docker/database/Dockerfile
	docker build -t ${DOCKERTAG} docker/bioflow
	docker build -t ${DBDOCKERTAG} docker/database

docker-clean: docker/bioflow/Dockerfile docker/bioflow/marathon.sh
	# Create the docker/bioflow binary in the Docker container using the
	# golang docker image. This ensures a completely clean build.
	$(DOCKER_GOLANG_RUN_CMD) "make docker/bioflow"
	docker build -t ${DOCKERTAG} docker

rpm/bioflow/bioflow: ${SRC}
	CGO_ENABLED=1 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/bioflow

rpm/biocli/biocli: ${SRC}
	CGO_ENABLED=1 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/biocli

rpm/bioadm/bioadm: ${SRC}
	CGO_ENABLED=1 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/bioadm

rpm/xtfscli/xtfscli: ${SRC}
	CGO_ENABLED=1 GOOS=linux go build -ldflags "${LDFLAGS}" -a -installsuffix cgo -o $@ github.com/xtao/bioflow/cmd/xtfscli

rpm: rpm/bioflow/bioflow rpm/biocli/biocli rpm/bioadm/bioadm rpm/xtfscli/xtfscli
	rm -rf rpm/biocli/examples.tar.gz
	tar zcvf rpm/biocli/examples.tar.gz etc/examples
	cd rpm && ./autogen.sh
	cd rpm && ./configure
	sed -i 's/XTAO_GIT_BIOFLOW_VERSION/$(BIOFLOW_VERSION)/g' rpm/Makefile
	cd rpm && make clean && make rpm

clean:
	rm -rf docker/bioflow/bioflow
	rm -rf docker/bioflow/biocli
	rm -rf docker/bioflow/bioadm
	rm -rf docker/bioflow/xtfscli
	rm -rf rpm/bioflow/bioflow
	rm -rf rpm/biocli/biocli
	rm -rf rpm/bioadm/bioadm
	rm -rf rpm/xtfscli/xtfscli
	rm -rf cmd/bioflow/bioflow
	rm -rf cmd/server/server
	rm -rf cmd/biocli/biocli
	rm -rf cmd/bioadm/bioadm
	rm -rf cmd/xtfscli/xtfscli

	rm -rf cmd/parser/parser
	rm -rf cmd/emulator/emulator
	rm -rf rpm/rpmbuild
	rm -rf rpm/config.*
	rm -rf rpm/configure
	rm -rf rpm/install.sh
	rm -rf rpm/Makefile
	rm -rf rpm/Makefile.in
	rm -rf rpm/*.tar.gz

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
