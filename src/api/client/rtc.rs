use std::time::SystemTime;

use axum::{
	Json,
	extract::State,
	response::IntoResponse,
};
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
};
use http::Uri;
use ruma::api::client::error::ErrorKind;
use serde::Deserialize;
use tuwunel_core::{Err, Error, Result, is_less_than};

#[derive(Deserialize, Default)]
struct AccessTokenQuery {
	access_token: Option<String>,
}

/// # `GET /_matrix/client/v1/rtc/transports`
///
/// Returns the configured MatrixRTC transports (MSC4143).
///
/// Requires authentication. Returns `{"rtc_transports": [...]}` when
/// transports are configured, or `{}` when none are configured.
pub(crate) async fn get_rtc_transports_route(
	State(services): State<crate::State>,
	bearer: Option<TypedHeader<Authorization<Bearer>>>,
	uri: Uri,
) -> Result<impl IntoResponse> {
	let query: AccessTokenQuery = uri
		.query()
		.map(serde_html_form::from_str)
		.transpose()
		.unwrap_or_default()
		.unwrap_or_default();

	let token = bearer
		.as_ref()
		.map(|TypedHeader(Authorization(bearer))| bearer.token())
		.or(query.access_token.as_deref());

	let Some(token) = token else {
		return Err!(Request(MissingToken("Missing access token.")));
	};

	match services.users.find_from_token(token).await {
		| Ok((user_id, device_id, expires_at)) => {
			if expires_at.is_some_and(is_less_than!(SystemTime::now())) {
				services.users.remove_access_token(&user_id, &device_id).await.ok();
				return Err(Error::BadRequest(
					ErrorKind::UnknownToken { soft_logout: true },
					"Expired access token.",
				));
			}
		},
		| Err(e) if e.is_not_found() => {
			services
				.appservice
				.find_from_access_token(token)
				.await
				.map_err(|_| {
					Error::BadRequest(
						ErrorKind::UnknownToken { soft_logout: false },
						"Unknown access token.",
					)
				})?;
		},
		| Err(e) => return Err(e),
	}

	let transports = &services.server.config.well_known.rtc_transports;

	if transports.is_empty() {
		return Ok(Json(serde_json::json!({})));
	}

	Ok(Json(serde_json::json!({
		"rtc_transports": transports,
	})))
}
