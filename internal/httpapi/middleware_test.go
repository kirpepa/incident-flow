package httpapi

import (
	"net/http"
	"testing"
)

func TestProtectedRouteLabelAvoidsIDCardinality(t *testing.T) {
	t.Parallel()
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodPost, "/v1/alerts", "POST /v1/alerts"},
		{http.MethodGet, "/v1/incidents", "GET /v1/incidents"},
		{http.MethodGet, "/v1/incidents/6d0d86d1/timeline", "GET /v1/incidents/{incidentID}/timeline"},
		{http.MethodPost, "/v1/incidents/6d0d86d1/ack", "POST /v1/incidents/{incidentID}/ack"},
	}
	for _, test := range tests {
		request, err := http.NewRequest(test.method, test.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Pattern = "/v1/"
		if got := routeLabel(request); got != test.want {
			t.Errorf("%s %s label = %q, want %q", test.method, test.path, got, test.want)
		}
	}
}
