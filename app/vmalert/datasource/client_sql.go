package datasource

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmalert/vmalertutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/xwb1989/sqlparser"
)

var (
	sqlAddr                  = flag.String("datasource.sql.url", "", "Optional URL for SQL datasource queries. If set, groups with type=\"sql\" will send queries to this URL instead of -datasource.url. Supports address in the form of IP address with a port (e.g., http://127.0.0.1:8428)")
	sqlBasicAuthUsername     = flag.String("datasource.sql.basicAuth.username", "", "Optional basic auth username for -datasource.sql.url")
	sqlBasicAuthPassword     = flag.String("datasource.sql.basicAuth.password", "", "Optional basic auth password for -datasource.sql.url")
	sqlBasicAuthPasswordFile = flag.String("datasource.sql.basicAuth.passwordFile", "", "Optional path to basic auth password to use for -datasource.sql.url")
	sqlBearerToken           = flag.String("datasource.sql.bearerToken", "", "Optional bearer auth token to use for -datasource.sql.url.")
	sqlBearerTokenFile       = flag.String("datasource.sql.bearerTokenFile", "", "Optional path to bearer token file to use for -datasource.sql.url.")
	sqlHeaders               = flag.String("datasource.sql.headers", "", "Optional HTTP extraHeaders to send with each request to the corresponding -datasource.sql.url. For example, -datasource.sql.headers='My-Auth:foobar' would send 'My-Auth: foobar' HTTP header with every request to the corresponding -datasource.sql.url. Multiple headers must be delimited by '^^': -datasource.sql.headers='header1:value1^^header2:value2'")
	sqlTLSInsecureSkipVerify = flag.Bool("datasource.sql.tlsInsecureSkipVerify", false, "Whether to skip tls verification when connecting to -datasource.sql.url")
	sqlTLSCertFile           = flag.String("datasource.sql.tlsCertFile", "", "Optional path to client-side TLS certificate file to use when connecting to -datasource.sql.url")
	sqlTLSKeyFile            = flag.String("datasource.sql.tlsKeyFile", "", "Optional path to client-side TLS certificate key to use when connecting to -datasource.sql.url")
	sqlTLSCAFile             = flag.String("datasource.sql.tlsCAFile", "", "Optional path to TLS CA file to use for verifying connections to -datasource.sql.url. By default, system CA is used")
	sqlTLSServerName         = flag.String("datasource.sql.tlsServerName", "", "Optional TLS server name to use for connections to -datasource.sql.url. By default, the server name from -datasource.sql.url is used")
)

const (
	sqPath              = "/sql"
	sqlValueAliasPrefix = "c_"
)

type sqlColumMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type sqlResponse struct {
	Meta []sqlColumMeta      `json:"meta"`
	Data [][]json.RawMessage `json:"data"`
}

// Router dispatches query building to the appropriate client
// based on the DataSourceType parameter.
type Router struct {
	defaultClient *Client
	sqlClient     *Client
}

// BuildWithParams dispatches to the SQL client for "sql" type,
// otherwise to the default client.
func (r *Router) BuildWithParams(params QuerierParams) Querier {
	if params.DataSourceType == string(datasourceSQL) {
		return r.sqlClient.BuildWithParams(params)
	}
	return r.defaultClient.BuildWithParams(params)
}

func newSQLClient() (*Client, error) {
	if *sqlAddr == "" {
		return nil, nil
	}
	if err := httputil.CheckURL(*sqlAddr); err != nil {
		return nil, fmt.Errorf("invalid -datasource.sql.url: %w", err)
	}

	tr, err := promauth.NewTLSTransport(
		*sqlTLSCertFile,
		*sqlTLSKeyFile,
		*sqlTLSCAFile,
		*sqlTLSServerName,
		*sqlTLSInsecureSkipVerify,
		"vmalert_sql_datasource",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport for -datasource.sql.url=%q: %w", *sqlAddr, err)
	}
	tr.DisableKeepAlives = *disableKeepAlive
	tr.MaxIdleConnsPerHost = *maxIdleConnections
	if tr.MaxIdleConns != 0 && tr.MaxIdleConns < tr.MaxIdleConnsPerHost {
		tr.MaxIdleConns = tr.MaxIdleConnsPerHost
	}
	tr.IdleConnTimeout = *idleConnectionTimeout

	authCfg, err := vmalertutil.AuthConfig(
		vmalertutil.WithBasicAuth(*sqlBasicAuthUsername, *sqlBasicAuthPassword, *sqlBasicAuthPasswordFile),
		vmalertutil.WithBearer(*sqlBearerToken, *sqlBearerTokenFile),
		vmalertutil.WithHeaders(*sqlHeaders),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to configure auth for -datasource.sql.url: %w", err)
	}
	if _, err := authCfg.GetAuthHeader(); err != nil {
		return nil, fmt.Errorf("failed to set request auth header to sql datasource %q: %w", *sqlAddr, err)
	}

	return &Client{
		c:              &http.Client{Transport: tr},
		authCfg:        authCfg,
		datasourceURL:  strings.TrimSuffix(*sqlAddr, "/"),
		dataSourceType: datasourceSQL,
		extraParams:    url.Values{},
	}, nil
}

func parseSQLNumericValue(raw json.RawMessage) (float64, error) {
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("cannot parse %q as numeric value", string(raw))
	}
	return strconv.ParseFloat(s, 64)
}

func normalizeSQLType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	for {
		switch {
		case strings.HasPrefix(t, "nullable(") && strings.HasSuffix(t, ")"):
			t = strings.TrimSuffix(strings.TrimPrefix(t, "nullable("), ")")
		case strings.HasPrefix(t, "lowcardinality(") && strings.HasSuffix(t, ")"):
			t = strings.TrimSuffix(strings.TrimPrefix(t, "lowcardinality("), ")")
		default:
			return t
		}
	}
}

func isNumericSQLType(t string) bool {
	t = normalizeSQLType(t)
	switch {
	case strings.HasPrefix(t, "int"),
		strings.HasPrefix(t, "uint"),
		strings.HasPrefix(t, "float"),
		strings.HasPrefix(t, "decimal"),
		strings.HasPrefix(t, "numeric"),
		strings.HasPrefix(t, "double"),
		strings.HasPrefix(t, "real"),
		strings.HasPrefix(t, "number"):
		return true
	default:
		return false
	}
}

func sqlEvalTimestamp(resp *http.Response) (int64, error) {
	if resp.Request == nil {
		return 0, fmt.Errorf("missing request in SQL response")
	}
	raw := resp.Request.URL.Query().Get("time")
	if raw == "" {
		return 0, fmt.Errorf("missing time query param in SQL request")
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, fmt.Errorf("cannot parse SQL evaluation time %q: %w", raw, err)
	}
	return t.Unix(), nil
}
func valueColumnIndex(meta []sqlColumMeta) (int, error) {
	idx := -1
	for i, col := range meta {
		if !strings.HasPrefix(strings.ToLower(col.Name), sqlValueAliasPrefix) {
			continue
		}
		if !isNumericSQLType(col.Type) {
			return -1, fmt.Errorf("column %q must be numeric", col.Name)
		}
		if idx != -1 {
			return -1, fmt.Errorf("multiple value columns with prefix %q", sqlValueAliasPrefix)
		}
		idx = i
	}
	if idx == -1 {
		return -1, fmt.Errorf("SQL response must contain exactly one numeric column with prefix %q", sqlValueAliasPrefix)
	}
	return idx, nil
}
func validateSQLQuery(query string) error {
	var trailingFormatRE = regexp.MustCompile(`(?is)\s+format\s+jsoncompact\s*$`)
	q := strings.TrimSpace(query)
	if q == "" {
		return fmt.Errorf("sql query cannot be empty")
	}
	q = trailingFormatRE.ReplaceAllString(q, "")
	stmt, err := sqlparser.Parse(q)
	if err != nil {
		return fmt.Errorf("invalid sql query: %w", err)
	}
	if _, ok := stmt.(*sqlparser.Select); !ok {
		return fmt.Errorf("only SELECT queries are allowed")
	}
	return nil
}
func parseSQLResponse(resp *http.Response) (Result, error) {
	var r sqlResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return Result{}, fmt.Errorf("error parsing SQL response: %w", err)
	}
	if len(r.Meta) == 0 {
		return Result{}, fmt.Errorf("SQL response has no columns")
	}
	valueIdx, err := valueColumnIndex(r.Meta)
	if err != nil {
		return Result{}, err
	}
	if valueIdx < 0 {
		return Result{}, fmt.Errorf("SQL response has no numeric column to use as metric value")
	}
	ts, err := sqlEvalTimestamp(resp)
	if err != nil {
		return Result{}, err
	}
	var metrics []Metric
	for _, row := range r.Data {
		if len(row) != len(r.Meta) {
			return Result{}, fmt.Errorf("SQL row has %d values, expected %d columns", len(row), len(r.Meta))
		}
		var m Metric
		val, err := parseSQLNumericValue(row[valueIdx])
		if err != nil {
			return Result{}, fmt.Errorf("error parsing value column %q: %w", r.Meta[valueIdx].Name, err)
		}
		m.Values = []float64{val}
		m.Timestamps = []int64{ts}
		for i, col := range r.Meta {
			if i == valueIdx {
				continue
			}
			var labelVal string
			if err := json.Unmarshal(row[i], &labelVal); err != nil {
				var numVal float64
				if err := json.Unmarshal(row[i], &numVal); err != nil {
					return Result{}, fmt.Errorf("error parsing label column %q: %w", col.Name, err)
				}
				labelVal = strconv.FormatFloat(numVal, 'f', -1, 64)
			}
			m.AddLabel(col.Name, labelVal)
		}
		metrics = append(metrics, m)
	}
	return Result{Data: metrics}, nil
}
func (c *Client) setSQLInstantReqParams(r *http.Request, query string, timestamp time.Time) error {
	if err := validateSQLQuery(query); err != nil {
		return err
	}
	if !*disablePathAppend {
		r.URL.Path += sqPath
	}
	q := r.URL.Query()
	q.Set("time", timestamp.Format(time.RFC3339))
	r.URL.RawQuery = q.Encode()
	c.setReqParams(r, query)
	return nil
}
