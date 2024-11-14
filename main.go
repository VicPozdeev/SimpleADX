package main

import (
	"SimpleADX/handler"
	"SimpleADX/repo"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	aeroDbHost := os.Getenv("AEROSPIKE_DB_HOST")
	aeroDbPort, err := strconv.Atoi(os.Getenv("AEROSPIKE_DB_PORT"))
	if err != nil {
		log.Fatalf("Invalid AEROSPIKE_DB_PORT: %v", err)
	}

	aeroCacheHost := os.Getenv("AEROSPIKE_CACHE_HOST")
	aeroCachePort, err := strconv.Atoi(os.Getenv("AEROSPIKE_CACHE_PORT"))
	if err != nil {
		log.Fatalf("Invalid AEROSPIKE_CACHE_PORT: %v", err)
	}

	clickhouseHost := os.Getenv("CLICKHOUSE_HOST")
	clickhousePort := os.Getenv("CLICKHOUSE_PORT")
	clickhouseUser := os.Getenv("CLICKHOUSE_USER")
	clickhousePassword := os.Getenv("CLICKHOUSE_PASSWORD")
	clickhouseDb := os.Getenv("CLICKHOUSE_DB")

	as, err := repo.NewAerospike(aeroDbHost, aeroDbPort,
		aeroCacheHost, aeroCachePort)
	if err != nil {
		panic(err)
	}

	ch, err := repo.NewClickhouse(clickhouseHost, clickhousePort,
		clickhouseUser, clickhousePassword, clickhouseDb)
	if err != nil {
		panic(err)
	}

	app := fiber.New()

	app.Use(limiter.New(limiter.Config{
		Max:               100,
		Expiration:        time.Second,
		LimiterMiddleware: limiter.SlidingWindow{},
	}))

	app.Post("/api/openrtb/:ssp_id", func(c *fiber.Ctx) error {
		return handler.OpenRTBHandler(c, as, ch)
	})

	log.Print("Start server")
	log.Fatal(app.Listen(":8080"))
}
