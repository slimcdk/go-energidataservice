package energidataservice

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
)

type client struct {
	resty *resty.Client
}

func New() client {
	r := resty.New()
	r.SetBaseURL("https://api.energidataservice.dk")
	return client{resty: r}
}

type stringListResponse struct {
	Success bool     `json:"success"`
	Result  []string `json:"result"`
}

func (c *client) Categories() ([]string, error) {
	var response stringListResponse
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&response).Get("/group_list")
	if err != nil {
		return nil, err
	}
	if res.StatusCode() != http.StatusOK {
		return nil, errors.New(res.Status())
	}
	return response.Result, nil
}

func (c *client) Tags() ([]string, error) {
	var response stringListResponse
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&response).Get("/tag_list")
	if err != nil {
		return nil, err
	}
	if res.StatusCode() != http.StatusOK {
		return nil, errors.New(res.Status())
	}
	return response.Result, nil
}

func (c *client) TagInfo(id string) (TagInfo, error) {
	var response tagInfoResponse
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&response).SetQueryParam("id", id).Get("/tag_show")
	if err != nil {
		return TagInfo{}, err
	}
	if res.StatusCode() != http.StatusOK {
		return TagInfo{}, errors.New(res.Status())
	}
	return response.Result, nil
}

func (c *client) Organizations() ([]string, error) {
	var response stringListResponse
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&response).Get("/organization_list")
	if err != nil {
		return nil, err
	}
	if res.StatusCode() != http.StatusOK {
		return nil, errors.New(res.Status())
	}
	return response.Result, nil
}

func (c *client) Packages() ([]string, error) {
	var response stringListResponse
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&response).Get("/package_list")
	if err != nil {
		return nil, err
	}
	if res.StatusCode() != http.StatusOK {
		return nil, errors.New(res.Status())
	}
	return response.Result, nil
}

type tagInfoResponse struct {
	Success bool    `json:"success"`
	Result  TagInfo `json:"result"`
}

type TagInfo struct {
	VocabularyID string `json:"vocabulary_id"`
	DisplayName  string `json:"display_name"`
	ID           string `json:"id"`
	Name         string `json:"name"`
}

func (c *client) DataStoreSQL(result interface{}, sql string) error {
	res, err := c.resty.R().SetHeader("accept", "application/json").SetResult(&result).SetQueryParam("sql", sql).Get("/datastore_search_sql")
	if err != nil {
		return err
	}
	if res.StatusCode() != http.StatusOK {
		return errors.New(res.Status())
	}
	return nil
}

func (c *client) Emissions(region string, from, to time.Time) error {

	statement := `
	WITH
	b AS (
	  SELECT
		"Minutes5UTC",
		date_trunc('hour', "Minutes5UTC") as hourutc,
		date_trunc('hour', "Minutes5DK") AS hourdk,
		"PriceArea",
		"CO2Emission"
	  FROM "co2emisprog"
	  WHERE "PriceArea" = 'DK1'
	  AND "Minutes5UTC" >= (current_timestamp at time zone 'UTC')
	  AND "Minutes5UTC" < ((current_timestamp at time zone 'UTC') %2B INTERVAL '6 hours')
	),
	a AS (
	  SELECT
		hourutc,
		CAST(AVG("CO2Emission") as INTEGER) AS CO2
	  FROM b
	  GROUP BY hourutc
	  ORDER BY hourutc ASC LIMIT 6
	)
	SELECT distinct to_char(b.hourDK, 'HH24:MI') AS "Minutes5DK", b."PriceArea", b.hourDK,
	a.CO2 as "CO2Emission"
	FROM a INNER JOIN b ON a.hourutc = b.hourutc
	ORDER BY b.hourDK ASC LIMIT 6
	`

	var res interface{}
	if err := c.DataStoreSQL(&res, statement); err != nil {
		return err
	}

	fmt.Println(res)

	return nil
}
