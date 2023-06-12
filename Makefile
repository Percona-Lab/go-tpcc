SHELL=bash
CONTAINER_NAME:=go-tpcc
REPOSITORY ?= lockecodes
VERSION ?= $(shell git describe --tags --always)
PODMAN ?= false
PODMAN_COMPOSE ?= false
DOCKER:="$(shell if ${PODMAN}; then \
		echo podman; \
		else echo docker; \
	fi \
)"
DOCKER_COMPOSE:="$(shell if ${PODMAN_COMPOSE}; then \
		echo podman-compose;\
		else echo docker-compose; \
	fi \
)"

EXPORTS=VERSION=${VERSION} \
		CONTAINER_NAME=${CONTAINER_NAME} \
		REPOSITORY=${REPOSITORY}

############################################################
##### Docker builds
############################################################
.PHONY: build-config
build-config:
	${EXPORTS} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		config

.PHONY: build
build: build-config
	${EXPORTS} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		build \
		tpcc
	VERSION=latest \
		CONTAINER_NAME=${CONTAINER_NAME} \
		REPOSITORY=${REPOSITORY} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		build \
		tpcc

.PHONY: push
push:
	${EXPORTS} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		push \
		tpcc
	VERSION=latest \
		CONTAINER_NAME=${CONTAINER_NAME} \
		REPOSITORY=${REPOSITORY} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		push \
		tpcc

.PHONY: shell
shell:
	${EXPORTS} \
		${DOCKER_COMPOSE} \
		-f docker-compose.yaml \
		-f docker-compose.dev.yaml \
		run \
			--rm \
			tpcc \
			bash
