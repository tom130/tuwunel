use axum::{Json, extract::State, response::IntoResponse};
use ruma::api::client::discovery::discover_support;
use serde::Serialize;
use serde_json::{Value as JsonValue, json};
use tuwunel_core::{Err, Result};

use crate::Ruma;

/// # `GET /.well-known/matrix/client`
///
/// Returns the .well-known URL if it is configured, otherwise returns 404.
/// Also includes RTC transport configuration for Element Call (MSC4143).
pub(crate) async fn well_known_client(
	State(services): State<crate::State>,
) -> Result<impl IntoResponse> {
	let homeserver_url = match services.config.well_known.client.as_ref() {
		| Some(url) => url.to_string(),
		| None => return Err!(Request(NotFound("Not found."))),
	};

	let rtc_foci = services.config.well_known.get_transports()?;
	let authentication_issuer = services
		.oauth
		.get_server()
		.ok()
		.and_then(|server| server.issuer_url().ok());

	Ok(Json(well_known_client_body(&homeserver_url, &rtc_foci, authentication_issuer)))
}

/// # `GET /.well-known/matrix/support`
///
/// Server support contact and support page of a homeserver's domain.
pub(crate) async fn well_known_support(
	State(services): State<crate::State>,
	_body: Ruma<discover_support::Request>,
) -> Result<discover_support::Response> {
	let config = &services.config.well_known;

	let support_page = config
		.support_page
		.as_ref()
		.map(ToString::to_string);

	let contacts = config.get_contacts();

	let policies = config.get_policies();

	if support_page.is_none() && contacts.is_empty() && policies.is_empty() {
		return Err!(Request(NotFound("Not found.")));
	}

	Ok(discover_support::Response { contacts, support_page, policies })
}

fn well_known_client_body<T: Serialize>(
	homeserver_url: &str,
	rtc_foci: &[T],
	authentication_issuer: Option<String>,
) -> JsonValue {
	let mut body = json!({
		"m.homeserver": {
			"base_url": homeserver_url,
		},
	});
	let object = body
		.as_object_mut()
		.expect("well-known body is a JSON object");

	if !rtc_foci.is_empty() {
		object.insert("org.matrix.msc4143.rtc_foci".into(), json!(rtc_foci));
	}

	if let Some(issuer) = authentication_issuer {
		object.insert("m.authentication".into(), json!({ "issuer": issuer }));
	}

	body
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::well_known_client_body;

	#[test]
	fn well_known_client_body_includes_oidc_authentication() {
		let body = well_known_client_body(
			"https://hs.example/",
			&Vec::<serde_json::Value>::new(),
			Some("https://hs.example/".into()),
		);

		assert_eq!(body["m.homeserver"]["base_url"], "https://hs.example/");
		assert_eq!(body["m.authentication"], json!({ "issuer": "https://hs.example/" }));
	}
}
