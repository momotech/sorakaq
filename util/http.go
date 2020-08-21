package util

import (
	"encoding/json"
	"errors"
	"net/http"
)

type Client struct {
	*http.Client
}

func NewClient(c *http.Client) *Client {
	return &Client{c}
}

func (c *Client) DoAsJson(req *http.Request, v interface{}) error {
	res, err := c.Do(req)
	return toJson(res, err, v)
}

func toJson(res *http.Response, err error, v interface{}) error {
	if res != nil {
		defer res.Body.Close()
	}
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New(res.Status)
	}
	return json.NewDecoder(res.Body).Decode(v)
}
