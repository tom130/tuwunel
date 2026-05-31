use std::time::SystemTime;

use axum::{
	body::Body,
	extract::{Path, State},
	response::Response,
};
use bytes::Bytes;
use http::{
	HeaderMap, HeaderValue, StatusCode,
	header::{
		CACHE_CONTROL, CONTENT_TYPE, ETAG, EXPIRES, HeaderName, IF_MATCH, IF_NONE_MATCH,
		LAST_MODIFIED, PRAGMA,
	},
};
use serde_json::json;
use tuwunel_core::utils::time;
use tuwunel_service::rendezvous::{RendezvousError, Session};

const ACCESS_CONTROL_ALLOW_HEADERS: HeaderName =
	HeaderName::from_static("access-control-allow-headers");
const ACCESS_CONTROL_ALLOW_METHODS: HeaderName =
	HeaderName::from_static("access-control-allow-methods");
const ACCESS_CONTROL_ALLOW_ORIGIN: HeaderName =
	HeaderName::from_static("access-control-allow-origin");
const ACCESS_CONTROL_EXPOSE_HEADERS: HeaderName =
	HeaderName::from_static("access-control-expose-headers");
const APPLICATION_JSON: &str = "application/json";
const CACHE_CONTROL_VALUE: &str = "no-store, no-transform";
const TEXT_PLAIN: &str = "text/plain";

/// # `POST /_matrix/client/unstable/org.matrix.msc4108/rendezvous`
///
/// Creates an opaque MSC4108 rendezvous session.
pub(crate) async fn create_rendezvous_session_route(
	State(services): State<crate::State>,
	headers: HeaderMap,
	body: Bytes,
) -> Response {
	if let Some(response) = next_gen_auth_gate_response(services.server.config.next_gen_auth) {
		return response;
	}

	if let Err(error) = services.rendezvous.check_rate_limit().await {
		return rendezvous_error_response(error);
	}

	if !is_text_plain(&headers) {
		return matrix_error_response(
			StatusCode::BAD_REQUEST,
			"M_INVALID_PARAM",
			"Content-Type must be text/plain.",
		);
	}

	match services.rendezvous.create(body, TEXT_PLAIN).await {
		| Ok(created) => {
			let public_base = public_base_url(services);
			let body = json!({
				"url": format!(
					"{public_base}/_matrix/client/unstable/org.matrix.msc4108/rendezvous/{}",
					created.id,
				),
			});

			json_session_response(StatusCode::CREATED, &created.session, &body)
		},
		| Err(error) => rendezvous_error_response(error),
	}
}

/// # `GET /_matrix/client/unstable/org.matrix.msc4108/rendezvous/{session_id}`
///
/// Retrieves an opaque MSC4108 rendezvous payload.
pub(crate) async fn get_rendezvous_session_route(
	State(services): State<crate::State>,
	Path(session_id): Path<String>,
	headers: HeaderMap,
) -> Response {
	if let Some(response) = next_gen_auth_gate_response(services.server.config.next_gen_auth) {
		return response;
	}

	if let Err(error) = services.rendezvous.check_rate_limit().await {
		return rendezvous_error_response(error);
	}

	match services.rendezvous.get(&session_id).await {
		| Ok(session) if if_none_match(&headers).is_some_and(|etag| etag == session.etag) =>
			not_modified_response(&session),
		| Ok(session) => session_headers_response(StatusCode::OK, &session, session.data.clone()),
		| Err(error) => rendezvous_error_response(error),
	}
}

/// # `PUT /_matrix/client/unstable/org.matrix.msc4108/rendezvous/{session_id}`
///
/// Updates an opaque MSC4108 rendezvous payload if the ETag matches.
pub(crate) async fn update_rendezvous_session_route(
	State(services): State<crate::State>,
	Path(session_id): Path<String>,
	headers: HeaderMap,
	body: Bytes,
) -> Response {
	if let Some(response) = next_gen_auth_gate_response(services.server.config.next_gen_auth) {
		return response;
	}

	if let Err(error) = services.rendezvous.check_rate_limit().await {
		return rendezvous_error_response(error);
	}

	if !is_text_plain(&headers) {
		return matrix_error_response(
			StatusCode::BAD_REQUEST,
			"M_INVALID_PARAM",
			"Content-Type must be text/plain.",
		);
	}

	let Some(if_match) = if_match(&headers) else {
		return matrix_error_response(
			StatusCode::BAD_REQUEST,
			"M_INVALID_PARAM",
			"If-Match header is required.",
		);
	};

	match services
		.rendezvous
		.update(&session_id, &if_match, body, TEXT_PLAIN)
		.await
	{
		| Ok(session) => accepted_empty_response(&session),
		| Err(RendezvousError::ConcurrentWrite) => concurrent_write_response(),
		| Err(error) => rendezvous_error_response(error),
	}
}

/// # `DELETE /_matrix/client/unstable/org.matrix.msc4108/rendezvous/{session_id}`
///
/// Deletes an MSC4108 rendezvous session.
pub(crate) async fn delete_rendezvous_session_route(
	State(services): State<crate::State>,
	Path(session_id): Path<String>,
) -> Response {
	if let Some(response) = next_gen_auth_gate_response(services.server.config.next_gen_auth) {
		return response;
	}

	if let Err(error) = services.rendezvous.check_rate_limit().await {
		return rendezvous_error_response(error);
	}

	match services.rendezvous.delete(&session_id).await {
		| Ok(()) => empty_response(StatusCode::NO_CONTENT),
		| Err(error) => rendezvous_error_response(error),
	}
}

fn public_base_url(services: crate::State) -> String {
	services
		.server
		.config
		.well_known
		.client
		.as_ref()
		.map(ToString::to_string)
		.unwrap_or_else(|| format!("https://{}", services.server.name))
		.trim_end_matches('/')
		.to_owned()
}

fn is_text_plain(headers: &HeaderMap) -> bool {
	headers
		.get(CONTENT_TYPE)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.split(';').next())
		.is_some_and(|essence| essence.trim().eq_ignore_ascii_case(TEXT_PLAIN))
}

fn if_match(headers: &HeaderMap) -> Option<String> {
	headers
		.get(IF_MATCH)
		.and_then(|value| value.to_str().ok())
		.map(ToOwned::to_owned)
}

fn if_none_match(headers: &HeaderMap) -> Option<&str> {
	headers
		.get(IF_NONE_MATCH)
		.and_then(|value| value.to_str().ok())
}

fn rendezvous_error_response(error: RendezvousError) -> Response {
	match error {
		| RendezvousError::NotFound => matrix_error_response(
			StatusCode::NOT_FOUND,
			"M_NOT_FOUND",
			"Rendezvous session not found.",
		),
		| RendezvousError::TooLarge => matrix_error_response(
			StatusCode::PAYLOAD_TOO_LARGE,
			"M_TOO_LARGE",
			"Rendezvous payload is too large.",
		),
		| RendezvousError::RateLimited => matrix_error_response(
			StatusCode::TOO_MANY_REQUESTS,
			"M_LIMIT_EXCEEDED",
			"Too many rendezvous requests.",
		),
		| RendezvousError::ConcurrentWrite => concurrent_write_response(),
	}
}

fn next_gen_auth_gate_response(next_gen_auth: bool) -> Option<Response> {
	(!next_gen_auth).then(unrecognized_response)
}

fn unrecognized_response() -> Response {
	matrix_error_response(StatusCode::NOT_FOUND, "M_UNRECOGNIZED", "Unrecognized request.")
}

fn concurrent_write_response() -> Response {
	let body = json!({
		"errcode": "M_UNKNOWN",
		"org.matrix.msc4108.errcode": "M_CONCURRENT_WRITE",
		"error": "Concurrent rendezvous write.",
	});

	json_response(StatusCode::PRECONDITION_FAILED, &body)
}

fn json_session_response(
	status: StatusCode,
	session: &Session,
	body: &serde_json::Value,
) -> Response {
	response_with_session_headers(
		status,
		APPLICATION_JSON,
		session,
		Bytes::from(body.to_string()),
	)
}

fn session_headers_response(status: StatusCode, session: &Session, body: Bytes) -> Response {
	response_with_session_headers(status, &session.content_type, session, body)
}

fn not_modified_response(session: &Session) -> Response {
	response_with_session_headers(StatusCode::NOT_MODIFIED, TEXT_PLAIN, session, Bytes::new())
}

fn accepted_empty_response(session: &Session) -> Response {
	response_with_session_headers(StatusCode::ACCEPTED, TEXT_PLAIN, session, Bytes::new())
}

fn response_with_session_headers(
	status: StatusCode,
	content_type: &str,
	session: &Session,
	body: Bytes,
) -> Response {
	let mut response = response(status, content_type, body);
	let headers = response.headers_mut();
	headers.insert(ETAG, header_value(&session.etag));
	headers.insert(EXPIRES, header_value(&http_date(session.expires)));
	headers.insert(LAST_MODIFIED, header_value(&http_date(session.last_modified)));
	response
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response {
	response(status, APPLICATION_JSON, Bytes::from(body.to_string()))
}

fn matrix_error_response(status: StatusCode, errcode: &str, error: &str) -> Response {
	let body = json!({ "errcode": errcode, "error": error });
	json_response(status, &body)
}

fn empty_response(status: StatusCode) -> Response {
	let mut response = Response::builder()
		.status(status)
		.body(Body::empty())
		.expect("failed to build response");
	add_rendezvous_headers(response.headers_mut());
	response
}

fn response(status: StatusCode, content_type: &str, body: Bytes) -> Response {
	let mut response = Response::builder()
		.status(status)
		.header(CONTENT_TYPE, content_type)
		.body(Body::from(body))
		.expect("failed to build response");
	add_rendezvous_headers(response.headers_mut());
	response
}

fn add_rendezvous_headers(headers: &mut HeaderMap) {
	headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
	headers.insert(ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET,PUT,POST,DELETE"));
	headers.insert(
		ACCESS_CONTROL_ALLOW_HEADERS,
		HeaderValue::from_static("Content-Type,If-Match,If-None-Match"),
	);
	headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("ETag"));
	headers.insert(CACHE_CONTROL, HeaderValue::from_static(CACHE_CONTROL_VALUE));
	headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
}

fn http_date(timepoint: SystemTime) -> String {
	time::format(timepoint, "%a, %d %b %Y %H:%M:%S GMT")
}

fn header_value(value: &str) -> HeaderValue {
	HeaderValue::from_str(value).expect("generated rendezvous header value is valid")
}

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use axum::body::to_bytes;
	use bytes::Bytes;
	use http::{
		HeaderMap, HeaderValue, StatusCode,
		header::{CONTENT_TYPE, HeaderName},
	};
	use serde_json::Value as JsonValue;
	use tuwunel_service::rendezvous::{RendezvousError, Session};

	use super::{
		accepted_empty_response, concurrent_write_response, is_text_plain, matrix_error_response,
		next_gen_auth_gate_response, rendezvous_error_response, session_headers_response,
	};

	#[test]
	fn rendezvous_content_type_validation_requires_text_plain() {
		let mut headers = HeaderMap::new();
		assert!(!is_text_plain(&headers));

		headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
		assert!(!is_text_plain(&headers));

		headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
		assert!(is_text_plain(&headers));

		headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
		assert!(is_text_plain(&headers));
	}

	#[tokio::test]
	async fn rendezvous_concurrent_write_error_has_msc4108_shape_and_cors() {
		let response = concurrent_write_response();
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::PRECONDITION_FAILED);
		assert_eq!(
			parts
				.headers
				.get(HeaderName::from_static("access-control-allow-origin")),
			Some(&HeaderValue::from_static("*")),
		);
		assert_eq!(
			parts
				.headers
				.get(HeaderName::from_static("access-control-expose-headers")),
			Some(&HeaderValue::from_static("ETag")),
		);

		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["errcode"], "M_UNKNOWN");
		assert_eq!(json["org.matrix.msc4108.errcode"], "M_CONCURRENT_WRITE");
	}

	#[tokio::test]
	async fn rendezvous_gate_off_returns_unrecognized_matrix_error() {
		assert!(next_gen_auth_gate_response(true).is_none());

		let response =
			next_gen_auth_gate_response(false).expect("disabled gate returns response");
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::NOT_FOUND);
		assert_eq!(
			parts
				.headers
				.get(HeaderName::from_static("access-control-allow-origin")),
			Some(&HeaderValue::from_static("*")),
		);

		let json = json_body(body).await;
		assert_eq!(json["errcode"], "M_UNRECOGNIZED");
	}

	#[tokio::test]
	async fn rendezvous_error_mapping_uses_matrix_errcodes() {
		let response = rendezvous_error_response(RendezvousError::TooLarge);
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::PAYLOAD_TOO_LARGE);
		assert_eq!(json_body(body).await["errcode"], "M_TOO_LARGE");

		let response = matrix_error_response(
			StatusCode::BAD_REQUEST,
			"M_INVALID_PARAM",
			"Content-Type must be text/plain.",
		);
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::BAD_REQUEST);
		assert_eq!(json_body(body).await["errcode"], "M_INVALID_PARAM");
	}

	#[test]
	fn rendezvous_accepted_empty_response_is_text_plain_with_session_headers() {
		let session = session();
		let response = accepted_empty_response(&session);
		assert_eq!(response.status(), StatusCode::ACCEPTED);
		assert_eq!(
			response.headers().get(CONTENT_TYPE),
			Some(&HeaderValue::from_static("text/plain")),
		);
		assert_eq!(
			response
				.headers()
				.get(HeaderName::from_static("etag")),
			Some(&HeaderValue::from_static("\"etag\"")),
		);
		assert_eq!(
			response
				.headers()
				.get(HeaderName::from_static("cache-control")),
			Some(&HeaderValue::from_static("no-store, no-transform")),
		);
	}

	#[test]
	fn rendezvous_session_responses_expose_etag_and_expiry_headers() {
		let response = session_headers_response(StatusCode::OK, &session(), Bytes::new());

		assert_eq!(
			response
				.headers()
				.get(HeaderName::from_static("etag")),
			Some(&HeaderValue::from_static("\"etag\"")),
		);
		assert!(
			response
				.headers()
				.contains_key(HeaderName::from_static("expires"))
		);
		assert!(
			response
				.headers()
				.contains_key(HeaderName::from_static("last-modified"))
		);
		assert_eq!(
			response
				.headers()
				.get(HeaderName::from_static("access-control-allow-methods")),
			Some(&HeaderValue::from_static("GET,PUT,POST,DELETE")),
		);
	}

	fn session() -> Session {
		let now = SystemTime::UNIX_EPOCH
			.checked_add(Duration::from_secs(1_000))
			.expect("test timestamp does not overflow");
		Session {
			etag: "\"etag\"".into(),
			data: Bytes::from_static(b"body"),
			content_type: "text/plain".into(),
			last_modified: now,
			expires: now
				.checked_add(Duration::from_secs(60))
				.expect("test timestamp does not overflow"),
		}
	}

	async fn json_body(body: axum::body::Body) -> JsonValue {
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		serde_json::from_slice(&body).expect("body is json")
	}
}
