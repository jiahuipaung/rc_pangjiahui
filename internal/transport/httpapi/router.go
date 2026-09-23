package httpapi

import "net/http"

type Dependencies struct {
	Creator      Creator
	CallerTokens map[string]string
}

func NewRouter(dependencies Dependencies) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/notifications", authenticate(dependencies.CallerTokens, http.HandlerFunc(createHandler(dependencies.Creator))))
	return mux
}
