use axum::{
	extract::State,
	response::{IntoResponse, Response},
};
use bytes::Bytes;
use http::StatusCode;
use ruma::api::client::error::ErrorKind;
use serde::Deserialize;
use tuwunel_core::{Error, Result};

#[derive(Debug, Deserialize)]
struct RevokeRequest {
	token: String,
	token_type_hint: Option<String>,
}

/// # `POST /_tuwunel/oauth/revoke`
///
/// Revokes a native Matrix access or refresh token using RFC 7009 semantics.
pub(crate) async fn oauth_revoke_route(
	State(services): State<crate::State>,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(revoke_disabled_error());
	}

	let body = std::str::from_utf8(&body)?;
	let request: RevokeRequest = serde_html_form::from_str(body)?;
	revoke_token(services, &request).await;

	Ok(revoke_ok_response())
}

async fn revoke_token(services: crate::State, request: &RevokeRequest) {
	let _token_type_hint = request.token_type_hint.as_deref();
	let Ok((user_id, device_id, ..)) = services
		.users
		.find_from_token(&request.token)
		.await
	else {
		return;
	};

	if request.token.starts_with("refresh_") {
		services
			.users
			.remove_refresh_token(&user_id, &device_id)
			.await
			.ok();
	} else {
		services
			.users
			.remove_access_token(&user_id, &device_id)
			.await
			.ok();
	}
}

fn revoke_ok_response() -> Response { StatusCode::OK.into_response() }

fn revoke_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
	use axum::body::to_bytes;
	use http::StatusCode;

	use super::{RevokeRequest, revoke_disabled_error, revoke_ok_response};

	#[tokio::test]
	async fn oauth_revoke_success_response_is_empty_200() {
		let response = revoke_ok_response();
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::OK);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		assert!(body.is_empty());
	}

	#[test]
	fn oauth_revoke_request_accepts_token_type_hint() {
		let request = RevokeRequest {
			token: "unknown".into(),
			token_type_hint: Some("refresh_token".into()),
		};

		assert_eq!(request.token, "unknown");
		assert_eq!(request.token_type_hint.as_deref(), Some("refresh_token"));
	}

	#[test]
	fn oauth_revoke_gate_off_error_is_unrecognized_404() {
		let error = revoke_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}
}
