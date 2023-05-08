deploy:
	helm --kubeconfig ~/.kube/config --kube-context docker-desktop upgrade \
		--install nats \
		--values=./deploy/helm/nats/values.yaml \
		--values=./deploy/helm/nats/values_$(env).yaml \
		--devel \
		./deploy/helm/nats

.PHONY: deploy