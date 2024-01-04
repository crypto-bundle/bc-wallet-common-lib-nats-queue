default: deploy_nats

deploy_nats:
	$(if $(and $(env),$(repository)),,$(error 'env' and/or 'repository' is not defined))

	$(eval context=$(or $(context),k0s-dev-cluster))
	$(eval platform=$(or $(platform),linux/amd64))

	helm --kube-context $(context) upgrade \
		--install nats \
		--values=./deploy/helm/nats/values.yaml \
		--values=./deploy/helm/nats/values_$(env).yaml \
		./deploy/helm/nats

destroy_nats:
	$(if $(and $(env),$(repository)),,$(error 'env' and/or 'repository' is not defined))

	$(eval context=$(or $(context),k0s-dev-cluster))
	$(eval platform=$(or $(platform),linux/amd64))

	helm --kube-context $(context) uninstall nats

.PHONY: deploy_nats