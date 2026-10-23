# Local CronJob demo

Requires Go, kind, Docker, and kubectl. The Makefile uses only the explicit `kind-cron-demo` context for cluster operations.

```sh
make up
make cron
make chaos
make cron
make heal
make down
```

The minute marker creates a small Job each minute. The parked export is suspended. Chaos adds a calendar with no returned date and a CronJob with an unspecified controller timezone. The latter illustrates uncertainty rather than claiming an unhealthy controller.

`make offline` explains the definitions without creating a cluster. Inputs rejected by Kubernetes admission, such as malformed expressions and invalid timezone names, are tested locally rather than submitted as pretend demo workloads.

The demo applies and deletes sample resources when explicitly invoked. The plugin's Overview, calendar, and watch read only CronJobs; richer commands also read related evidence and logs. `make check` uses fixtures and local HTTP servers; it does not start Docker or mutate a cluster. The opt-in `make e2e` suite creates and deletes its own isolated cluster and never touches this demo.
