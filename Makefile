CONTAINER=demopy
MAJOR_REV_FILE=major-revision.txt
MINOR_REV_FILE=minor-revision.txt
BUILD_REV_FILE=build-revision.txt

.PHONY: build push checkenv container

container: checkenv push
checkenv:
ifndef REGISTRY
	$(error REGISTRY is undefined)
endif
push:
	@if ! test -f $(BUILD_REV_FILE); then echo 0 > $(BUILD_REV_FILE); fi
	@echo $$(($$(cat $(BUILD_REV_FILE)) + 1)) > $(BUILD_REV_FILE)
	@if ! test -f $(MAJOR_REV_FILE); then echo 1 > $(MAJOR_REV_FILE); fi
	@if ! test -f $(MINOR_REV_FILE); then echo 0 > $(MINOR_REV_FILE); fi
	$(eval MAJOR_REV := $(shell cat $(MAJOR_REV_FILE)))
	$(eval MINOR_REV := $(shell cat $(MINOR_REV_FILE)))
	$(eval BUILD_REV := $(shell cat $(BUILD_REV_FILE)))
	docker build --force-rm=true --no-cache=true -t $(CONTAINER) -f Dockerfile .
	docker image tag $(CONTAINER):latest $(REGISTRY)/$(CONTAINER):latest
	docker image tag $(CONTAINER):latest $(REGISTRY)/$(CONTAINER):$(MAJOR_REV).$(MINOR_REV).$(BUILD_REV)
	docker push $(REGISTRY)/$(CONTAINER):latest
	docker push $(REGISTRY)/$(CONTAINER):$(MAJOR_REV).$(MINOR_REV).$(BUILD_REV)
build:
	docker build --force-rm=true --no-cache=true -t $(CONTAINER) -f Dockerfile .
