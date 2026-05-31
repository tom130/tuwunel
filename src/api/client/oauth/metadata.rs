use axum::{
	Json,
	extract::State,
	response::{IntoResponse, Response},
};
use http::StatusCode;
use ruma::api::client::error::ErrorKind;
use serde_json::{Value as JsonValue, json};
use tuwunel_core::{Error, Result};

const DEVICE_CODE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// # `GET /_matrix/client/v1/auth_metadata`
///
/// Returns OAuth authorization-server metadata for Matrix next-generation auth.
pub(crate) async fn get_auth_metadata_route(
	State(services): State<crate::State>,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(auth_metadata_disabled_error());
	}

	Ok(Json(auth_metadata_body(&public_base_url(services))).into_response())
}

fn auth_metadata_body(public_base: &str) -> JsonValue {
	let public_base = public_base.trim_end_matches('/');

	json!({
		"issuer": public_base,
		"authorization_endpoint": format!("{public_base}/_tuwunel/oauth/authorize"),
		"token_endpoint": format!("{public_base}/_tuwunel/oauth/token"),
		"registration_endpoint": format!("{public_base}/_tuwunel/oauth/register"),
		"device_authorization_endpoint": format!("{public_base}/_tuwunel/oauth/device"),
		"revocation_endpoint": format!("{public_base}/_tuwunel/oauth/revoke"),
		"response_types_supported": ["code"],
		"grant_types_supported": [
			"authorization_code",
			"refresh_token",
			DEVICE_CODE_GRANT,
		],
		"response_modes_supported": ["query", "fragment"],
		"code_challenge_methods_supported": ["S256"],
	})
}

fn auth_metadata_disabled_error() -> Error {
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
	use http::StatusCode;

	use super::{auth_metadata_body, auth_metadata_disabled_error};

	#[test]
	fn auth_metadata_advertises_device_authorization_grant() {
		let metadata = auth_metadata_body("https://hs.example");

		assert_eq!(metadata["issuer"], "https://hs.example");
		assert_eq!(
			metadata["device_authorization_endpoint"],
			"https://hs.example/_tuwunel/oauth/device",
		);
		assert!(
			metadata["grant_types_supported"]
				.as_array()
				.expect("grant_types_supported is an array")
				.iter()
				.any(|grant| grant == "urn:ietf:params:oauth:grant-type:device_code"),
		);
	}

	#[test]
	fn auth_metadata_gate_off_error_is_unrecognized_404() {
		let error = auth_metadata_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}
}
