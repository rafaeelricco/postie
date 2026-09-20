package sourcepg

import (
	"net/url"
	"strconv"

	"github.com/rafaeelricco/postie/internal/stream"
)

// Connection contains source database credentials and connection settings.
type Connection struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// Client inspects one configured PostgreSQL source.
type Client struct {
	Source     stream.Source
	Connection Connection
}

func connString(c Connection) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(c.Username, c.Password), Host: c.Host + ":" + strconv.Itoa(c.Port), Path: "/" + c.Database}
	return u.String()
}
