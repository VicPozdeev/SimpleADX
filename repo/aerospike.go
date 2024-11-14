package repo

import (
	"SimpleADX/model"
	"encoding/json"
	"errors"
	"fmt"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"log"
)

type Aerospike struct {
	cacheClient   *aero.Client
	storageClient *aero.Client
}

func NewAerospike(dbHost string, dbPort int, cacheHost string, cachePort int) (*Aerospike, error) {
	storageClient, asErr := aero.NewClient(dbHost, dbPort)
	if asErr != nil {
		return nil, asErr
	}

	cacheClient, asErr := aero.NewClient(cacheHost, cachePort)
	if asErr != nil {
		return nil, asErr
	}

	db := &Aerospike{
		cacheClient:   cacheClient,
		storageClient: storageClient,
	}

	err := initTestRecords(db)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize test records: %w", err)
	}

	return db, nil
}

func initTestRecords(db *Aerospike) error {
	err := db.storageClient.Truncate(nil, "db", "exchange", nil)
	if err != nil {
		return fmt.Errorf("failed to truncate set: %w", err)
	}

	err = db.cacheClient.Truncate(nil, "cache", "exchange", nil)
	if err != nil {
		return fmt.Errorf("failed to truncate set: %w", err)
	}

	task, err := db.storageClient.CreateIndex(nil, "db", "exchange", "idx_ssp_id", "ssp_id", aero.STRING)
	if err != nil {
		return fmt.Errorf("error creating index: %w", err)
	}

	if err := <-task.OnComplete(); err != nil {
		return fmt.Errorf("index creation task failed: %w", err)
	}

	exchangeRecords := []model.ExchangeRecord{
		{SSPID: "1", DSPID: "1", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "1", DSPID: "2", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "1", DSPID: "3", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "1", DSPID: "4", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "1", DSPID: "5", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "2", DSPID: "1", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "2", DSPID: "2", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "2", DSPID: "3", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "2", DSPID: "4", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "2", DSPID: "5", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "3", DSPID: "4", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "3", DSPID: "2", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "3", DSPID: "6", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "4", DSPID: "1", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "4", DSPID: "3", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "5", DSPID: "5", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "5", DSPID: "6", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "5", DSPID: "3", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "6", DSPID: "1", Commission: 0.2, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
		{SSPID: "6", DSPID: "2", Commission: 0.1, DSPURL: "https://devssp.al-adtech.com/api/debug/openrtb/test_response?dsp_id=test"},
	}

	for _, record := range exchangeRecords {
		key, err := aero.NewKey("db", "exchange", record.SSPID+"_"+record.DSPID)
		if err != nil {
			log.Println("Failed to create key:", err)
			continue
		}

		binData := aero.BinMap{
			"ssp_id":     record.SSPID,
			"dsp_id":     record.DSPID,
			"commission": record.Commission,
			"dsp_url":    record.DSPURL,
		}

		err = db.storageClient.Put(nil, key, binData)
		if err != nil {
			log.Println("Failed to insert record:", err)
		}
	}

	log.Println("Aerospike database successfully initialized.")
	return nil
}

func (db *Aerospike) GetExchangeRecords(sspID string) ([]model.ExchangeRecord, error) {
	cacheRecords, err := db.getFromCache(sspID)
	if err != nil {
		log.Println("Cache error:", err)
	}
	if cacheRecords != nil {
		return cacheRecords, nil
	}

	records, err := db.getFromPersistentStore(sspID)
	if err != nil {
		return nil, err
	}

	err = db.saveToCache(sspID, records)
	if err != nil {
		log.Println("Failed to save to cache:", err)
	}

	return records, nil
}

func (db *Aerospike) getFromPersistentStore(sspID string) ([]model.ExchangeRecord, error) {
	stmt := aero.NewStatement("db", "exchange")
	err := stmt.SetFilter(aero.NewEqualFilter("ssp_id", sspID))
	if err != nil {
		return nil, err
	}

	recordset, err := db.storageClient.Query(nil, stmt)
	if err != nil {
		return nil, err
	}
	defer recordset.Close()

	var records []model.ExchangeRecord
	for res := range recordset.Results() {
		if res.Err != nil {
			log.Println("Error reading record:", res.Err)
			continue
		}

		record := model.ExchangeRecord{
			SSPID:      res.Record.Bins["ssp_id"].(string),
			DSPID:      res.Record.Bins["dsp_id"].(string),
			Commission: res.Record.Bins["commission"].(float64),
			DSPURL:     res.Record.Bins["dsp_url"].(string),
		}
		records = append(records, record)
	}

	return records, nil
}

func (db *Aerospike) getFromCache(sspID string) ([]model.ExchangeRecord, error) {
	key, err := aero.NewKey("cache", "exchange", sspID)
	if err != nil {
		return nil, err
	}

	rec, err := db.cacheClient.Get(nil, key)
	if err != nil || rec == nil {
		if errors.Is(err, aero.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	var records []model.ExchangeRecord
	if cachedData, ok := rec.Bins["records"].([]byte); ok {
		if err := json.Unmarshal(cachedData, &records); err != nil {
			log.Println("Failed to unmarshal cached data:", err)
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("cached data not found")
	}

	return records, nil
}

func (db *Aerospike) saveToCache(sspID string, records []model.ExchangeRecord) error {
	key, asErr := aero.NewKey("cache", "exchange", sspID)
	if asErr != nil {
		return asErr
	}

	recordsJSON, err := json.Marshal(records)
	if err != nil {
		return err
	}

	binData := aero.BinMap{
		"records": recordsJSON,
	}

	writePolicy := aero.NewWritePolicy(0, 60)
	return db.cacheClient.Put(writePolicy, key, binData)
}
