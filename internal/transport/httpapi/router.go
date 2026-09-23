package httpapi

import "net/http"

type Dependencies struct {
	Creator      Creator
	Querier      Querier
	Replayer     Replayer
	CallerTokens map[string]string
	AdminTokens  map[string]string
}

func NewRouter(dependencies Dependencies) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/notifications", authenticate(dependencies.CallerTokens, http.HandlerFunc(createHandler(dependencies.Creator))))
	mux.Handle("GET /v1/notifications/{notification_id}", authenticate(dependencies.CallerTokens, http.HandlerFunc(queryHandler(dependencies.Querier))))
	mux.Handle("POST /v1/admin/notifications/{notification_id}/replay", authenticate(dependencies.AdminTokens, http.HandlerFunc(replayHandler(dependencies.Replayer))))
	return mux
}
