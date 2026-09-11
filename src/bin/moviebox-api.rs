use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use moviebox_tui::{
    providers::{MediaType, ProviderKind, ProviderError},
    service::MovieBoxService,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

#[derive(Clone)]
struct AppState { service: Arc<MovieBoxService> }

#[derive(Deserialize)] struct SearchQuery { q: Option<String>, provider: Option<String>, page: Option<usize> }
#[derive(Deserialize)] struct EpisodeQuery { season: Option<usize> }
#[derive(Serialize)] struct ErrorBody { error: ErrorDetail }
#[derive(Serialize)] struct ErrorDetail { code: &'static str, message: String }

fn error(status: StatusCode, code: &'static str, message: impl Into<String>) -> Response {
    (status, Json(ErrorBody { error: ErrorDetail { code, message: message.into() } })).into_response()
}
fn encoded(provider: ProviderKind, value: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{}|{}", provider.cache_key(), value))
}
fn decode_id(id: &str) -> Result<(ProviderKind, String), Response> {
    let bytes = URL_SAFE_NO_PAD.decode(id).map_err(|_| error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Malformed title ID"))?;
    let raw = String::from_utf8(bytes).map_err(|_| error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Malformed title ID"))?;
    let (provider, value) = raw.split_once('|').ok_or_else(|| error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Malformed title ID"))?;
    let provider = ProviderKind::parse(provider).ok_or_else(|| error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Unsupported provider"))?;
    if value.trim().is_empty() || value.len() > 512 { return Err(error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Malformed title ID")); }
    Ok((provider, value.to_string()))
}
fn provider_error(e: ProviderError) -> Response {
    let (status, code) = match e { ProviderError::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND"), ProviderError::RateLimited(_) => (StatusCode::TOO_MANY_REQUESTS, "RATE_LIMITED"), ProviderError::Network(_) => (StatusCode::BAD_GATEWAY, "UPSTREAM_ERROR"), ProviderError::Parsing(_) | ProviderError::Unavailable(_) => (StatusCode::BAD_GATEWAY, "PROVIDER_ERROR") };
    error(status, code, "Provider request failed")
}
fn media_type(t: MediaType) -> &'static str { match t { MediaType::Movie => "movie", MediaType::Series => "series" } }

async fn health() -> Json<serde_json::Value> { Json(serde_json::json!({"status":"ok","service":"moviebox-api"})) }
async fn api_info() -> Json<serde_json::Value> { Json(serde_json::json!({"name":"MovieBox API","version":"1.0.0","endpoints":{"health":"/health","search":"/api/v1/search?q=...","title":"/api/v1/title/{id}","episodes":"/api/v1/title/{id}/episodes","stream":"/api/v1/stream/{id}","providers":"/api/v1/providers"}})) }

async fn providers(State(state): State<AppState>) -> Json<serde_json::Value> {
    let items: Vec<_> = [ProviderKind::MovieBox, ProviderKind::FourKHdHub, ProviderKind::BdixCircleFtp, ProviderKind::BdixDhakaFlix, ProviderKind::Addons].into_iter().map(|p| { let c = state.service.capabilities(p); serde_json::json!({"id":p.cache_key(),"name":p.label(),"search":c.supports_search,"details":true,"episodes":c.supports_series,"streams":matches!(p, ProviderKind::MovieBox|ProviderKind::FourKHdHub|ProviderKind::BdixCircleFtp|ProviderKind::BdixDhakaFlix)}) }).collect();
    Json(serde_json::json!({"providers":items}))
}

async fn search(State(state): State<AppState>, Query(params): Query<SearchQuery>) -> Response {
    let q = params.q.unwrap_or_default(); let q = q.trim();
    if q.is_empty() || q.len() > 120 { return error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Search query must be 1-120 characters"); }
    let page = params.page.unwrap_or(1); if page == 0 || page > 100 { return error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Page must be between 1 and 100"); }
    let provider = match params.provider.as_deref() { Some(p) => match ProviderKind::parse(p) { Some(p) => p, None => return error(StatusCode::BAD_REQUEST, "INVALID_REQUEST", "Unsupported provider") }, None => ProviderKind::MovieBox };
    match tokio::time::timeout(Duration::from_secs(20), state.service.search_typed(provider, q, page)).await {
        Ok(Ok(items)) => Json(serde_json::json!({"query":q,"results":items.into_iter().take(50).map(|i| serde_json::json!({"id":encoded(i.id.provider,&i.id.value),"title":i.title,"type":media_type(i.media_type),"year":i.year,"poster":i.poster_url,"provider":i.id.provider.cache_key()})).collect::<Vec<_>>() })).into_response(),
        Ok(Err(e)) => provider_error(e), Err(_) => error(StatusCode::GATEWAY_TIMEOUT, "UPSTREAM_ERROR", "Provider request timed out"),
    }
}

async fn title(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let (provider, value) = match decode_id(&id) { Ok(v) => v, Err(r) => return r };
    match tokio::time::timeout(Duration::from_secs(20), state.service.details_typed(provider, &value)).await {
        Ok(Ok(d)) => Json(serde_json::json!({"id":encoded(provider,&value),"title":d.title,"original_title":null,"type":media_type(d.media_type),"year":d.year,"poster":d.poster_url,"backdrop":null,"overview":d.description,"genres":d.genres,"rating":d.imdb_rating.and_then(|v|v.parse::<f64>().ok()),"runtime":d.duration,"status":null,"provider":provider.cache_key(),"seasons":d.seasons})).into_response(),
        Ok(Err(e)) => provider_error(e), Err(_) => error(StatusCode::GATEWAY_TIMEOUT, "UPSTREAM_ERROR", "Provider request timed out"),
    }
}

async fn episodes(State(state): State<AppState>, Path(id): Path<String>, Query(query): Query<EpisodeQuery>) -> Response {
    let (provider, value) = match decode_id(&id) { Ok(v) => v, Err(r) => return r };
    match tokio::time::timeout(Duration::from_secs(20), state.service.details_typed(provider, &value)).await {
        Ok(Ok(d)) => {
            let seasons = if let Some(s) = query.season {
                d.seasons.into_iter().filter(|x| x.number == s).collect()
            } else {
                d.seasons
            };
            Json(serde_json::json!({"title_id":encoded(provider,&value),"seasons":seasons})).into_response()
        },
        Ok(Err(e)) => provider_error(e), Err(_) => error(StatusCode::GATEWAY_TIMEOUT, "UPSTREAM_ERROR", "Provider request timed out"),
    }
}

async fn stream(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let bytes = match URL_SAFE_NO_PAD.decode(&id) { Ok(v) => v, Err(_) => return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Malformed stream ID") };
    let raw = match String::from_utf8(bytes) { Ok(v)=>v, Err(_)=>return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Malformed stream ID") };
    let parts: Vec<_> = raw.split('|').collect(); if parts.len()!=4 { return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Stream ID must identify provider, title, season, and episode"); }
    let provider = match ProviderKind::parse(parts[0]) { Some(p)=>p, None=>return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Unsupported provider") }; let season: usize=match parts[2].parse(){Ok(v) if v>0=>v,_=>return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Invalid season")}; let episode: usize=match parts[3].parse(){Ok(v) if v>0=>v,_=>return error(StatusCode::BAD_REQUEST,"INVALID_REQUEST","Invalid episode")};
    match tokio::time::timeout(Duration::from_secs(35), state.service.episode_streams_typed(provider,parts[1],season,episode)).await { Ok(Ok(releases)) => { let sources = releases.into_iter().flat_map(|r| { let quality = r.quality.clone(); let codec = r.codec.clone(); r.mirrors.into_iter().map(move |m| serde_json::json!({"url":m.resolver_url,"quality":quality,"type":"unknown","format":codec,"label":m.label})) }).collect::<Vec<_>>(); Json(serde_json::json!({"id":id,"title":null,"sources":sources,"subtitles":[]})).into_response() }, Ok(Err(e))=>provider_error(e), Err(_)=>error(StatusCode::GATEWAY_TIMEOUT,"RESOLUTION_FAILED","Stream resolution timed out") }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    moviebox_tui::logging::init();
    let state = AppState { service: Arc::new(MovieBoxService::new()) };
    let cors = match std::env::var("ALLOWED_ORIGINS") { Ok(v) => { let origins = v.split(',').filter_map(|s| s.trim().parse::<HeaderValue>().ok()).collect::<Vec<_>>(); CorsLayer::new().allow_origin(origins).allow_methods([axum::http::Method::GET]).allow_headers([header::CONTENT_TYPE]) }, Err(_) => CorsLayer::very_permissive() };
    let app = Router::new().route("/health", get(health)).route("/api", get(api_info)).route("/api/v1/providers", get(providers)).route("/api/v1/search", get(search)).route("/api/v1/title/{id}", get(title)).route("/api/v1/title/{id}/episodes", get(episodes)).route("/api/v1/stream/{id}", get(stream)).with_state(state).layer(cors).layer(TraceLayer::new_for_http());
    let port: u16 = std::env::var("PORT").unwrap_or_else(|_| "3000".into()).parse()?; let addr = SocketAddr::from(([0,0,0,0],port)); let listener = tokio::net::TcpListener::bind(addr).await?; log::info!("moviebox-api listening on {addr}"); axum::serve(listener, app).await?; Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn ids_round_trip() { let id=encoded(ProviderKind::MovieBox,"abc:123"); let (p,v)=decode_id(&id).unwrap(); assert_eq!(p,ProviderKind::MovieBox); assert_eq!(v,"abc:123"); }
}
