use axum::{
	Json,
	extract::State,
	response::{IntoResponse, Response},
};
use bytes::Bytes;
use http::StatusCode;
use ruma::api::client::error::ErrorKind;
use serde::Deserialize;
use serde_json::json;
use tuwunel_core::{Error, Result};
use tuwunel_service::oauth_provider::{grants::CreatedGrant, scope::parse_scope};

#[derive(Deserialize)]
struct DeviceAuthorizationRequest {
	client_id: String,
	scope: String,
}

/// # `POST /_tuwunel/oauth/device`
///
/// Starts an OAuth device-authorization grant for Matrix QR login.
pub(crate) async fn device_authorization_route(
	State(services): State<crate::State>,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(device_authorization_disabled_error());
	}

	let body = std::str::from_utf8(&body)?;
	let request: DeviceAuthorizationRequest = serde_html_form::from_str(body)?;

	match services
		.oauth_provider
		.clients
		.get(&request.client_id)
		.await
	{
		| Ok(_) => {},
		| Err(error) if error.is_not_found() => {
			return Ok(oauth_error_response(StatusCode::BAD_REQUEST, "invalid_client"));
		},
		| Err(error) => return Err(error),
	}

	let Ok(granted_scope) = parse_scope(&request.scope) else {
		return Ok(oauth_error_response(StatusCode::BAD_REQUEST, "invalid_scope"));
	};

	let created = services
		.oauth_provider
		.grants
		.create(request.client_id, granted_scope)
		.await?;
	let verification_uri = format!("{}/_tuwunel/oauth/link", public_base_url(services));

	Ok(device_authorization_response(&created, &verification_uri))
}

fn device_authorization_response(grant: &CreatedGrant, verification_uri: &str) -> Response {
	let expires_in = grant
		.grant
		.expires
		.duration_since(grant.grant.created)
		.map_or(0, |duration| duration.as_secs());

	(
		StatusCode::OK,
		Json(json!({
			"device_code": grant.device_code,
			"user_code": grant.user_code,
			"verification_uri": verification_uri,
			"verification_uri_complete": format!("{verification_uri}?user_code={}", grant.user_code),
			"expires_in": expires_in,
			"interval": grant.grant.interval.as_secs(),
		})),
	)
		.into_response()
}

fn oauth_error_response(status: StatusCode, error: &str) -> Response {
	(status, Json(json!({ "error": error }))).into_response()
}

fn device_authorization_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
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

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use axum::body::to_bytes;
	use http::StatusCode;
	use serde_json::Value as JsonValue;
	use tuwunel_service::oauth_provider::{
		grants::{CreatedGrant, GrantStatus, PendingGrant},
		scope::parse_scope,
	};

	use super::{
		device_authorization_disabled_error, device_authorization_response, oauth_error_response,
	};

	#[tokio::test]
	async fn device_authorization_response_has_rfc8628_fields() {
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let response = device_authorization_response(
			&CreatedGrant {
				device_code: "device-code".into(),
				user_code: "ABCD-EFGH".into(),
				grant: PendingGrant {
					client_id: "client".into(),
					granted_scope: parse_scope(
						"urn:matrix:client:api:* urn:matrix:client:device:DEVICE",
					)
					.expect("scope parses"),
					device_id: "DEVICE".into(),
					user_code: "ABCD-EFGH".into(),
					created: now,
					expires: now + Duration::from_secs(600),
					interval: Duration::from_secs(5),
					last_polled: None,
					status: GrantStatus::Pending,
				},
			},
			"https://hs.example/_tuwunel/oauth/link",
		);

		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::OK);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["device_code"], "device-code");
		assert_eq!(json["user_code"], "ABCD-EFGH");
		assert_eq!(
			json["verification_uri_complete"],
			"https://hs.example/_tuwunel/oauth/link?user_code=ABCD-EFGH",
		);
		assert_eq!(json["expires_in"], 600);
		assert_eq!(json["interval"], 5);
	}

	#[tokio::test]
	async fn device_authorization_oauth_errors_use_error_field() {
		let response = oauth_error_response(StatusCode::BAD_REQUEST, "invalid_client");
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::BAD_REQUEST);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["error"], "invalid_client");
	}

	#[test]
	fn device_authorization_gate_off_error_is_unrecognized_404() {
		let error = device_authorization_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}
}
