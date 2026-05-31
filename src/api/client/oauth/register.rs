use axum::{
	Json,
	extract::State,
	response::{IntoResponse, Response},
};
use bytes::Bytes;
use http::StatusCode;
use ruma::api::client::error::ErrorKind;
use tuwunel_core::{Error, Result};
use tuwunel_service::oauth_provider::clients::{ClientRegistrationRequest, RegisteredClient};

/// # `POST /_tuwunel/oauth/register`
///
/// Registers a public OAuth client for the next-generation auth device grant.
pub(crate) async fn register_oauth_client_route(
	State(services): State<crate::State>,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(next_gen_auth_disabled_error());
	}

	let request: ClientRegistrationRequest = serde_json::from_slice(&body)?;
	let registered = services
		.oauth_provider
		.clients
		.register(request)
		.await?;

	Ok(client_registration_response(&registered))
}

fn client_registration_response(client: &RegisteredClient) -> Response {
	(StatusCode::CREATED, Json(client.registration_response())).into_response()
}

fn next_gen_auth_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
	use axum::body::to_bytes;
	use http::StatusCode;
	use serde_json::Value as JsonValue;
	use tuwunel_service::oauth_provider::clients::{ClientMetadata, RegisteredClient};

	use super::{client_registration_response, next_gen_auth_disabled_error};

	#[tokio::test]
	async fn oauth_register_created_response_flattens_client_metadata() {
		let response = client_registration_response(&RegisteredClient {
			client_id: "client123".into(),
			client_id_issued_at: 1234,
			metadata: ClientMetadata {
				client_name: Some("Element X".into()),
				client_uri: "https://client.example/".into(),
				redirect_uris: Vec::new(),
				application_type: "web".into(),
				token_endpoint_auth_method: "none".into(),
				grant_types: vec![
					"urn:ietf:params:oauth:grant-type:device_code".into(),
					"refresh_token".into(),
				],
				response_types: vec!["code".into()],
				localized_client_names: [("client_name#en".into(), "Element X".into())].into(),
			},
		});
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::CREATED);

		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["client_id"], "client123");
		assert_eq!(json["client_id_issued_at"], 1234);
		assert_eq!(json["client_uri"], "https://client.example/");
		assert_eq!(json["client_name#en"], "Element X");
	}

	#[test]
	fn oauth_register_gate_off_error_is_unrecognized_404() {
		let error = next_gen_auth_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}
}
