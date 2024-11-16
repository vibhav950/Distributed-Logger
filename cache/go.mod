module example.com/cache

go 1.23.3

replace example.com/logger => ../logger

require example.com/logger v0.0.0-00010101000000-000000000000

require github.com/google/uuid v1.6.0 // indirect
