default: deploy

deploy:
	helm --kube-context $(cluster_name) upgrade \
		--install nats \
		--values=./deploy/helm/nats/values.yaml \
		--values=./deploy/helm/nats/values_$(env).yaml \
		--devel \
		./deploy/helm/nats

.PHONY: deploy