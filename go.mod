module github.com/secrets-bridge/worker

go 1.25.0

// During active polyrepo development we pin api via a local replace so
// cross-repo changes don't require a tag-and-publish cycle. Switch to
// real semver once api's pkg surface stabilizes.
replace github.com/secrets-bridge/api => ../api

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/prometheus/client_golang v1.23.2
	github.com/secrets-bridge/api v0.0.0-20260528232131-e8a63b09826e
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang-migrate/migrate/v4 v4.19.1 // indirect
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/redis/go-redis/v9 v9.20.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
