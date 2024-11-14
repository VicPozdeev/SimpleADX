package repo

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type Clickhouse struct {
	db *sql.DB
}

func NewClickhouse(host, port, username, password, database string) (*Clickhouse, error) {
	db, err := sql.Open("clickhouse",
		fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
			username, password, host, port, database))
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	err = initDatabase(db)
	if err != nil {
		return nil, err
	}

	return &Clickhouse{db: db}, nil
}

func initDatabase(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS auction_stats")
	if err != nil {
		return fmt.Errorf("error dropping existing table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE auction_stats (
			ssp_id String,
			dsp_id String,
			ssp_bidfloor Float64,
			dsp_price Float64,
			commission Float64,
			timestamp DateTime
		) ENGINE = MergeTree()
		ORDER BY timestamp
	`)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}

	log.Println("Clickhouse database successfully initialized.")
	return nil
}

func (c *Clickhouse) InsertStats(sspID, dspID string, sspBidFloor, dspPrice, commission float64) error {
	query := `
		INSERT INTO auction_stats (ssp_id, dsp_id, ssp_bidfloor, dsp_price, commission, timestamp)
		VALUES (?, ?, ?, ?, ?, now())
	`
	_, err := c.db.Exec(query, sspID, dspID, sspBidFloor, dspPrice, commission)
	if err != nil {
		log.Printf("Failed to insert auction stats: %v", err)
		return err
	}

	return nil
}
