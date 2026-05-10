package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHealthz_ReturnsExactBody enforces acceptance test 5: the body is the
// exact string {"status":"ok"} with no trailing newline. Writing the body
// via json.Encode would append a newline and break this.
func TestHealthz_ReturnsExactBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()

	healthz(rr, req)

	resp := rr.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body))
}
