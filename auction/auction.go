package auction

import (
	"SimpleADX/model"
	"encoding/json"
	"fmt"
	"github.com/prebid/openrtb/v19/openrtb2"
	"github.com/valyala/fasthttp"
	"log"
	"math"
)

type ExchangeRepo interface {
	GetExchangeRecords(sspID string) ([]model.ExchangeRecord, error)
}

type StatisticRepo interface {
	InsertStats(sspID, dspID string, sspBidFloor, dspPrice, commission float64) error
}

func ProcessAuction(er ExchangeRepo, sr StatisticRepo, sspID string, bidRequest *openrtb2.BidRequest) (*openrtb2.BidResponse, error) {
	records, err := er.GetExchangeRecords(sspID)
	if err != nil {
		return nil, err
	}

	if len(bidRequest.Imp) == 0 {
		return nil, nil
	}
	bidFloor := bidRequest.Imp[0].BidFloor
	var bestBid *openrtb2.Bid
	var bidRecord model.ExchangeRecord
	var highestPrice float64

	for _, record := range records {
		minPrice := roundFloat(bidFloor * (1 + record.Commission))
		bidRequest.Imp[0].BidFloor = minPrice
		bid, err := sendDSPRequest(record.DSPURL, bidRequest)
		if err != nil || bid == nil {
			continue
		}

		if bid.Price > minPrice && bid.Price > highestPrice {
			bestBid = bid
			bidRecord = record
			highestPrice = bid.Price
		}
	}

	if bestBid == nil {
		return nil, nil
	}

	err = sr.InsertStats(bidRecord.SSPID, bidRecord.DSPID,
		bidFloor, bestBid.Price, bidRecord.Commission)
	if err != nil {
		log.Println(err)
	}

	bestBid.Price = bidFloor

	return &openrtb2.BidResponse{
		ID: bidRequest.ID,
		SeatBid: []openrtb2.SeatBid{
			{Bid: []openrtb2.Bid{
				*bestBid,
			}},
		},
	}, nil
}

func roundFloat(val float64) float64 {
	return math.Round(val*1000) / 1000
}

func sendDSPRequest(dspURL string, bidRequest *openrtb2.BidRequest) (*openrtb2.Bid, error) {
	reqBody, err := json.Marshal(bidRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bid request: %w", err)
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")
	req.SetRequestURI(dspURL)
	req.SetBody(reqBody)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		return nil, fmt.Errorf("failed to send request to DSP: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return nil, fmt.Errorf("DSP returned non-OK status: %d", resp.StatusCode())
	}

	var bidResponse openrtb2.BidResponse
	if err := json.Unmarshal(resp.Body(), &bidResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal DSP response: %w", err)
	}

	if len(bidResponse.SeatBid) == 0 || len(bidResponse.SeatBid[0].Bid) == 0 {
		return nil, fmt.Errorf("no bid found in DSP response")
	}

	return &bidResponse.SeatBid[0].Bid[0], nil
}
