SHELL := /bin/bash

SERVICES := integration_api integration_consumer_webhook integration_consumer_channels

lint-fix:
	@for svc in $(SERVICES); do \
		echo "→ $$svc lint-fix"; \
		$(MAKE) -C $$svc lint-fix; \
	done

test:
	@for svc in $(SERVICES); do \
		echo "→ $$svc test"; \
		$(MAKE) -C $$svc test; \
	done
