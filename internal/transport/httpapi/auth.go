package httpapi

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
)

type callerContextKey struct{}

func authenticate(tokens map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		provided := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		caller := ""
		for token, identity := range tokens {
			if len(token) == len(provided) && subtle.ConstantTimeCompare([]byte(token), []byte(provided)) == 1 {
				caller = identity
			}
		}
		if caller == "" {
			writeError(writer, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		next.ServeHTTP(writer, request.WithContext(context.WithValue(request.Context(), callerContextKey{}, caller)))
	})
}

func callerFromContext(ctx context.Context) string {
	caller, _ := ctx.Value(callerContextKey{}).(string)
	return caller
}
