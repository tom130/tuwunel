use std::{
	collections::BTreeMap,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tuwunel_core::{Err, Result, err, implement, utils::random_string};
use tuwunel_database::{Cbor, Deserialized, Map};
use url::Url;

const CLIENT_ID_LENGTH: usize = 32;
const DEVICE_CODE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
const REFRESH_TOKEN_GRANT: &str = "refresh_token";
const AUTHORIZATION_CODE_GRANT: &str = "authorization_code";
const CODE_RESPONSE_TYPE: &str = "code";
const TOKEN_ENDPOINT_AUTH_METHOD_NONE: &str = "none";
const DEFAULT_APPLICATION_TYPE: &str = "web";

pub struct Clients {
	db: Data,
}

struct Data {
	oauthclient_metadata: Arc<Map>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRegistrationRequest {
	pub client_name: Option<String>,
	pub client_uri: Option<String>,
	#[serde(default)]
	pub redirect_uris: Vec<String>,
	pub application_type: Option<String>,
	pub token_endpoint_auth_method: Option<String>,
	#[serde(default)]
	pub grant_types: Vec<String>,
	#[serde(default)]
	pub response_types: Vec<String>,
	#[serde(flatten)]
	pub extra_metadata: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientMetadata {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub client_name: Option<String>,
	pub client_uri: String,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub redirect_uris: Vec<String>,
	pub application_type: String,
	pub token_endpoint_auth_method: String,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub grant_types: Vec<String>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub response_types: Vec<String>,
	#[serde(flatten)]
	pub localized_client_names: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegisteredClient {
	pub client_id: String,
	pub client_id_issued_at: u64,
	pub metadata: ClientMetadata,
}

#[implement(Clients)]
pub(super) fn build(args: &crate::Args<'_>) -> Self {
	Self {
		db: Data {
			oauthclient_metadata: args.db["oauthclient_metadata"].clone(),
		},
	}
}

#[implement(Clients)]
#[tracing::instrument(level = "debug", skip(self), ret)]
pub async fn register(&self, request: ClientRegistrationRequest) -> Result<RegisteredClient> {
	let metadata = ClientMetadata::try_from(request)?;
	let client_id = self.unused_client_id().await?;
	let registered = RegisteredClient {
		client_id,
		client_id_issued_at: issued_at_now(),
		metadata,
	};

	self.db
		.oauthclient_metadata
		.raw_put(registered.client_id.as_str(), Cbor(&registered));

	Ok(registered)
}

#[implement(Clients)]
#[tracing::instrument(level = "debug", skip(self), ret)]
pub async fn get(&self, client_id: &str) -> Result<RegisteredClient> {
	self.db
		.oauthclient_metadata
		.get(client_id)
		.await
		.deserialized::<Cbor<_>>()
		.map(|registered| registered.0)
}

#[implement(Clients)]
async fn unused_client_id(&self) -> Result<String> {
	loop {
		let client_id = random_string(CLIENT_ID_LENGTH);
		match self.get(&client_id).await {
			| Ok(_) => continue,
			| Err(error) if error.is_not_found() => return Ok(client_id),
			| Err(error) => return Err(error),
		}
	}
}

impl TryFrom<ClientRegistrationRequest> for ClientMetadata {
	type Error = tuwunel_core::Error;

	fn try_from(request: ClientRegistrationRequest) -> Result<Self> {
		let client_uri = request
			.client_uri
			.ok_or_else(|| err!(Request(InvalidParam("client_uri is required."))))?;
		validate_https_url("client_uri", &client_uri)?;

		for redirect_uri in &request.redirect_uris {
			validate_url("redirect_uri", redirect_uri)?;
		}

		let token_endpoint_auth_method = request
			.token_endpoint_auth_method
			.unwrap_or_else(|| TOKEN_ENDPOINT_AUTH_METHOD_NONE.into());
		if token_endpoint_auth_method != TOKEN_ENDPOINT_AUTH_METHOD_NONE {
			return Err!(Request(InvalidParam("token_endpoint_auth_method must be none.",)));
		}

		let mut localized_client_names = BTreeMap::new();
		for (name, value) in request.extra_metadata {
			if !name.starts_with("client_name#") {
				continue;
			}

			let Some(value) = value.as_str() else {
				return Err!(Request(InvalidParam(
					"localized client_name values must be strings.",
				)));
			};

			localized_client_names.insert(name, value.to_owned());
		}

		Ok(Self {
			client_name: request.client_name,
			client_uri,
			redirect_uris: request.redirect_uris,
			application_type: request
				.application_type
				.unwrap_or_else(|| DEFAULT_APPLICATION_TYPE.into()),
			token_endpoint_auth_method,
			grant_types: filter_supported(request.grant_types, [
				DEVICE_CODE_GRANT,
				REFRESH_TOKEN_GRANT,
				AUTHORIZATION_CODE_GRANT,
			]),
			response_types: filter_supported(request.response_types, [CODE_RESPONSE_TYPE]),
			localized_client_names,
		})
	}
}

impl RegisteredClient {
	#[must_use]
	pub fn registration_response(&self) -> serde_json::Value {
		let mut value = serde_json::to_value(&self.metadata)
			.expect("client metadata serializes to JSON object");
		let object = value
			.as_object_mut()
			.expect("client metadata serializes to JSON object");

		object.insert("client_id".into(), self.client_id.clone().into());
		object.insert("client_id_issued_at".into(), self.client_id_issued_at.into());

		value
	}
}

fn filter_supported<const N: usize>(values: Vec<String>, supported: [&str; N]) -> Vec<String> {
	values
		.into_iter()
		.filter(|value| supported.contains(&value.as_str()))
		.collect()
}

fn validate_https_url(field: &'static str, value: &str) -> Result {
	let url = validate_url(field, value)?;
	if url.scheme() != "https" {
		return Err!(Request(InvalidParam("{field} must use https.")));
	}

	Ok(())
}

fn validate_url(field: &'static str, value: &str) -> Result<Url> {
	Url::parse(value).map_err(|_| err!(Request(InvalidParam("{field} must be a valid URL."))))
}

fn issued_at_now() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
	use super::{ClientMetadata, ClientRegistrationRequest, JsonValue};

	#[test]
	fn oauth_register_normalizes_public_client_metadata() {
		let metadata = ClientMetadata::try_from(ClientRegistrationRequest {
			client_name: Some("Element X".into()),
			client_uri: Some("https://client.example/".into()),
			redirect_uris: Vec::new(),
			application_type: None,
			token_endpoint_auth_method: Some("none".into()),
			grant_types: vec![
				"urn:ietf:params:oauth:grant-type:device_code".into(),
				"unsupported".into(),
				"refresh_token".into(),
			],
			response_types: vec!["code".into(), "token".into()],
			extra_metadata: [("client_name#en".into(), JsonValue::String("Element X".into()))]
				.into(),
		})
		.expect("public client metadata is valid");

		assert_eq!(metadata.client_name.as_deref(), Some("Element X"));
		assert_eq!(metadata.client_uri, "https://client.example/");
		assert_eq!(metadata.application_type, "web");
		assert_eq!(metadata.token_endpoint_auth_method, "none");
		assert_eq!(metadata.grant_types, [
			"urn:ietf:params:oauth:grant-type:device_code".to_owned(),
			"refresh_token".to_owned(),
		],);
		assert_eq!(metadata.response_types, ["code".to_owned()]);
		assert_eq!(
			metadata
				.localized_client_names
				.get("client_name#en"),
			Some(&"Element X".to_owned()),
		);
	}

	#[test]
	fn oauth_register_rejects_missing_or_non_https_client_uri() {
		ClientMetadata::try_from(ClientRegistrationRequest::default()).unwrap_err();

		ClientMetadata::try_from(ClientRegistrationRequest {
			client_uri: Some("http://client.example/".into()),
			..Default::default()
		})
		.unwrap_err();
	}

	#[test]
	fn oauth_register_ignores_unknown_top_level_metadata() {
		let request: ClientRegistrationRequest = serde_json::from_value(serde_json::json!({
			"client_uri": "https://client.example/",
			"contacts": ["ops@example"],
			"client_name#en": "Element X"
		}))
		.expect("unknown non-string metadata does not break deserialization");

		let metadata = ClientMetadata::try_from(request).expect("metadata is valid");
		assert_eq!(
			metadata
				.localized_client_names
				.get("client_name#en"),
			Some(&"Element X".to_owned()),
		);
	}
}
