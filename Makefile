.PHONY: setup cluster deploy watch demo scale down clean test

CLUSTER := watchline

setup:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -e ".[dev]"

cluster:
	kind create cluster --name $(CLUSTER) --config k8s/kind-config.yaml

deploy:
	kubectl apply -f k8s/watchline-demo.yaml

watch:
	.venv/bin/watchline --namespace watchline

demo:
	kubectl -n watchline rollout status deployment/pod-demo
	kubectl -n watchline get pods,service

scale:
	kubectl -n watchline scale deployment/pod-demo --replicas=3

down:
	kubectl delete -f k8s/watchline-demo.yaml

clean:
	kind delete cluster --name $(CLUSTER)

test:
	.venv/bin/python -m pytest

