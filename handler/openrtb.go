package handler

import (
	"SimpleADX/auction"
	"github.com/gofiber/fiber/v2"
	"github.com/prebid/openrtb/v19/openrtb2"
)

func OpenRTBHandler(c *fiber.Ctx, er auction.ExchangeRepo, sr auction.StatisticRepo) error {
	sspID := c.Params("ssp_id")
	var bidRequest openrtb2.BidRequest

	if err := c.BodyParser(&bidRequest); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid request"})
	}

	bidResponse, err := auction.ProcessAuction(er, sr, sspID, &bidRequest)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(bidResponse)
}
