use std::{
	collections::HashMap,
	sync::Arc,
	time::{Duration, SystemTime},
};

use ruma::OwnedUserId;
use tokio::sync::Mutex;
use tuwunel_core::{Result, utils::random_string};

use super::scope::GrantedScope;

const AUTH_ID_LENGTH: usize = 32;
const CODE_LENGTH: usize = 32;
const MAX_AUTHORIZE_STORE_ENTRIES: usize = 1024;

pub struct Authorizations {
	pub pending: PendingAuthorizations,
	pub codes: AuthorizationCodes,
}

impl Authorizations {
	pub(super) fn build(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			pending: PendingAuthorizations::new(
				Duration::from_secs(args.server.config.consent_session_ttl_secs),
				MAX_AUTHORIZE_STORE_ENTRIES,
			),
			codes: AuthorizationCodes::new(
				Duration::from_secs(args.server.config.auth_code_ttl_secs),
				MAX_AUTHORIZE_STORE_ENTRIES,
			),
		})
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingAuthorization {
	pub client_id: String,
	pub redirect_uri: String,
	pub granted_scope: GrantedScope,
	pub device_id: String,
	pub code_challenge: String,
	pub state: String,
	pub created: SystemTime,
	pub expires: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedPendingAuthorization {
	pub auth_id: String,
	pub authorization: PendingAuthorization,
}

#[derive(Default)]
struct PendingState {
	authorizations: HashMap<String, PendingAuthorization>,
}

pub struct PendingAuthorizations {
	state: Mutex<PendingState>,
	expires_after: Duration,
	max_entries: usize,
}

impl PendingAuthorizations {
	fn new(expires_after: Duration, max_entries: usize) -> Self {
		Self {
			state: Mutex::new(PendingState::default()),
			expires_after,
			max_entries: max_entries.max(1),
		}
	}

	#[cfg(test)]
	fn new_for_testing(expires_after: Duration, max_entries: usize) -> Arc<Self> {
		Arc::new(Self::new(expires_after, max_entries))
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn create<ClientId, RedirectUri, CodeChallenge, RequestState>(
		&self,
		client_id: ClientId,
		redirect_uri: RedirectUri,
		granted_scope: GrantedScope,
		code_challenge: CodeChallenge,
		state: RequestState,
	) -> Result<CreatedPendingAuthorization>
	where
		ClientId: Into<String>,
		RedirectUri: Into<String>,
		CodeChallenge: Into<String>,
		RequestState: Into<String>,
	{
		self.create_at(
			client_id,
			redirect_uri,
			granted_scope,
			code_challenge,
			state,
			SystemTime::now(),
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn create_at<ClientId, RedirectUri, CodeChallenge, RequestState>(
		&self,
		client_id: ClientId,
		redirect_uri: RedirectUri,
		granted_scope: GrantedScope,
		code_challenge: CodeChallenge,
		request_state: RequestState,
		now: SystemTime,
	) -> Result<CreatedPendingAuthorization>
	where
		ClientId: Into<String>,
		RedirectUri: Into<String>,
		CodeChallenge: Into<String>,
		RequestState: Into<String>,
	{
		let mut store = self.state.lock().await;
		Self::evict_expired_state(&mut store, now);
		self.evict_to_capacity(&mut store);

		let auth_id = unused_key(&store.authorizations, AUTH_ID_LENGTH);
		let authorization = PendingAuthorization {
			client_id: client_id.into(),
			redirect_uri: redirect_uri.into(),
			device_id: granted_scope.device_id.clone(),
			granted_scope,
			code_challenge: code_challenge.into(),
			state: request_state.into(),
			created: now,
			expires: now.checked_add(self.expires_after).unwrap_or(now),
		};

		store
			.authorizations
			.insert(auth_id.clone(), authorization.clone());
		Ok(CreatedPendingAuthorization { auth_id, authorization })
	}

	pub async fn get(&self, auth_id: &str) -> Result<PendingAuthorization> {
		self.get_at(auth_id, SystemTime::now()).await
	}

	pub async fn get_at(&self, auth_id: &str, now: SystemTime) -> Result<PendingAuthorization> {
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, now);
		state
			.authorizations
			.get(auth_id)
			.cloned()
			.ok_or_else(not_found)
	}

	pub async fn consume(&self, auth_id: &str) -> Result<PendingAuthorization> {
		self.consume_at(auth_id, SystemTime::now()).await
	}

	pub async fn consume_at(
		&self,
		auth_id: &str,
		now: SystemTime,
	) -> Result<PendingAuthorization> {
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, now);
		state
			.authorizations
			.remove(auth_id)
			.ok_or_else(not_found)
	}

	pub async fn evict_expired(&self) {
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, SystemTime::now());
	}

	fn evict_expired_state(state: &mut PendingState, now: SystemTime) {
		state
			.authorizations
			.retain(|_, authorization| now < authorization.expires);
	}

	fn evict_to_capacity(&self, state: &mut PendingState) {
		while state.authorizations.len() >= self.max_entries {
			let Some(oldest) = state
				.authorizations
				.iter()
				.min_by_key(|(_, authorization)| authorization.created)
				.map(|(auth_id, _)| auth_id.clone())
			else {
				break;
			};

			state.authorizations.remove(&oldest);
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationCode {
	pub client_id: String,
	pub redirect_uri: String,
	pub granted_scope: GrantedScope,
	pub device_id: String,
	pub user_id: OwnedUserId,
	pub code_challenge: String,
	pub created: SystemTime,
	pub expires: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedAuthorizationCode {
	pub code: String,
	pub authorization: AuthorizationCode,
}

#[derive(Default)]
struct CodesState {
	codes: HashMap<String, AuthorizationCode>,
}

pub struct AuthorizationCodes {
	state: Mutex<CodesState>,
	expires_after: Duration,
	max_entries: usize,
}

impl AuthorizationCodes {
	fn new(expires_after: Duration, max_entries: usize) -> Self {
		Self {
			state: Mutex::new(CodesState::default()),
			expires_after,
			max_entries: max_entries.max(1),
		}
	}

	#[cfg(test)]
	fn new_for_testing(expires_after: Duration, max_entries: usize) -> Arc<Self> {
		Arc::new(Self::new(expires_after, max_entries))
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn create<ClientId, RedirectUri, CodeChallenge>(
		&self,
		client_id: ClientId,
		redirect_uri: RedirectUri,
		granted_scope: GrantedScope,
		user_id: OwnedUserId,
		code_challenge: CodeChallenge,
	) -> Result<CreatedAuthorizationCode>
	where
		ClientId: Into<String>,
		RedirectUri: Into<String>,
		CodeChallenge: Into<String>,
	{
		self.create_at(
			client_id,
			redirect_uri,
			granted_scope,
			user_id,
			code_challenge,
			SystemTime::now(),
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn create_at<ClientId, RedirectUri, CodeChallenge>(
		&self,
		client_id: ClientId,
		redirect_uri: RedirectUri,
		granted_scope: GrantedScope,
		user_id: OwnedUserId,
		code_challenge: CodeChallenge,
		now: SystemTime,
	) -> Result<CreatedAuthorizationCode>
	where
		ClientId: Into<String>,
		RedirectUri: Into<String>,
		CodeChallenge: Into<String>,
	{
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, now);
		self.evict_to_capacity(&mut state);

		let code = unused_key(&state.codes, CODE_LENGTH);
		let authorization = AuthorizationCode {
			client_id: client_id.into(),
			redirect_uri: redirect_uri.into(),
			device_id: granted_scope.device_id.clone(),
			granted_scope,
			user_id,
			code_challenge: code_challenge.into(),
			created: now,
			expires: now.checked_add(self.expires_after).unwrap_or(now),
		};

		state
			.codes
			.insert(code.clone(), authorization.clone());
		Ok(CreatedAuthorizationCode { code, authorization })
	}

	pub async fn consume(&self, code: &str) -> Result<AuthorizationCode> {
		self.consume_at(code, SystemTime::now()).await
	}

	pub async fn consume_at(&self, code: &str, now: SystemTime) -> Result<AuthorizationCode> {
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, now);
		state.codes.remove(code).ok_or_else(not_found)
	}

	pub async fn evict_expired(&self) {
		let mut state = self.state.lock().await;
		Self::evict_expired_state(&mut state, SystemTime::now());
	}

	fn evict_expired_state(state: &mut CodesState, now: SystemTime) {
		state
			.codes
			.retain(|_, authorization| now < authorization.expires);
	}

	fn evict_to_capacity(&self, state: &mut CodesState) {
		while state.codes.len() >= self.max_entries {
			let Some(oldest) = state
				.codes
				.iter()
				.min_by_key(|(_, authorization)| authorization.created)
				.map(|(code, _)| code.clone())
			else {
				break;
			};

			state.codes.remove(&oldest);
		}
	}
}

fn unused_key<T>(map: &HashMap<String, T>, length: usize) -> String {
	loop {
		let key = random_string(length);
		if !map.contains_key(&key) {
			return key;
		}
	}
}

fn not_found() -> tuwunel_core::Error {
	tuwunel_core::err!(Request(NotFound("OAuth authorization not found.")))
}

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use ruma::owned_user_id;

	use super::{AuthorizationCodes, PendingAuthorizations};
	use crate::oauth_provider::scope::parse_scope;

	fn now() -> SystemTime {
		SystemTime::UNIX_EPOCH
			.checked_add(Duration::from_secs(1_000))
			.expect("test timestamp is representable")
	}

	#[tokio::test]
	async fn pending_authorization_create_get_consume_is_single_use() {
		let store = PendingAuthorizations::new_for_testing(Duration::from_secs(600), 8);
		let scope = parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
			.expect("scope parses");

		let created = store
			.create_at(
				"client",
				"https://app.example/callback",
				scope,
				"challenge",
				"opaque-state",
				now(),
			)
			.await
			.expect("pending authorization creates");

		assert_eq!(created.authorization.client_id, "client");
		assert_eq!(created.authorization.redirect_uri, "https://app.example/callback");
		assert_eq!(created.authorization.device_id, "DEVICE");
		assert_eq!(created.authorization.code_challenge, "challenge");
		assert_eq!(created.authorization.state, "opaque-state");
		assert_eq!(created.authorization.expires, now() + Duration::from_secs(600),);

		let looked_up = store
			.get_at(&created.auth_id, now())
			.await
			.expect("pending authorization is readable");
		assert_eq!(looked_up, created.authorization);

		let consumed = store
			.consume_at(&created.auth_id, now())
			.await
			.expect("pending authorization consumes");
		assert_eq!(consumed, created.authorization);
		store
			.consume_at(&created.auth_id, now())
			.await
			.expect_err("pending authorization is single-use");
	}

	#[tokio::test]
	async fn pending_authorization_expires_and_capacity_evicts_oldest() {
		let store = PendingAuthorizations::new_for_testing(Duration::from_secs(10), 1);
		let scope = parse_scope("urn:matrix:client:api:* urn:matrix:client:device:OLD")
			.expect("scope parses");
		let old = store
			.create_at("client", "https://app.example/old", scope, "old", "state", now())
			.await
			.expect("old pending authorization creates");

		store
			.get_at(&old.auth_id, now() + Duration::from_secs(11))
			.await
			.expect_err("expired pending authorization is evicted");

		let first = store
			.create_at(
				"client",
				"https://app.example/first",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:FIRST")
					.expect("scope parses"),
				"first",
				"state",
				now(),
			)
			.await
			.expect("first pending authorization creates");
		let second = store
			.create_at(
				"client",
				"https://app.example/second",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:SECOND")
					.expect("scope parses"),
				"second",
				"state",
				now() + Duration::from_secs(1),
			)
			.await
			.expect("second pending authorization creates");

		store
			.get_at(&first.auth_id, now() + Duration::from_secs(1))
			.await
			.expect_err("oldest pending authorization is evicted at capacity");
		store
			.get_at(&second.auth_id, now() + Duration::from_secs(1))
			.await
			.expect("newest pending authorization remains");
	}

	#[tokio::test]
	async fn authorization_code_create_consume_is_single_use_and_preserves_fields() {
		let store = AuthorizationCodes::new_for_testing(Duration::from_secs(60), 8);
		let user_id = owned_user_id!("@alice:example.com");
		let scope = parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
			.expect("scope parses");

		let created = store
			.create_at(
				"client",
				"http://127.0.0.1:12345/callback",
				scope.clone(),
				user_id.clone(),
				"challenge",
				now(),
			)
			.await
			.expect("authorization code creates");

		assert_eq!(created.authorization.client_id, "client");
		assert_eq!(created.authorization.redirect_uri, "http://127.0.0.1:12345/callback");
		assert_eq!(created.authorization.granted_scope, scope);
		assert_eq!(created.authorization.device_id, "DEVICE");
		assert_eq!(created.authorization.user_id, user_id);
		assert_eq!(created.authorization.code_challenge, "challenge");
		assert_eq!(created.authorization.expires, now() + Duration::from_secs(60));

		let consumed = store
			.consume_at(&created.code, now())
			.await
			.expect("authorization code consumes");
		assert_eq!(consumed, created.authorization);
		store
			.consume_at(&created.code, now())
			.await
			.expect_err("authorization code is single-use");
	}

	#[tokio::test]
	async fn authorization_code_expires_and_capacity_evicts_oldest() {
		let store = AuthorizationCodes::new_for_testing(Duration::from_secs(10), 1);
		let user_id = owned_user_id!("@alice:example.com");
		let expired = store
			.create_at(
				"client",
				"https://app.example/expired",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:EXPIRED")
					.expect("scope parses"),
				user_id.clone(),
				"expired",
				now(),
			)
			.await
			.expect("authorization code creates");

		store
			.consume_at(&expired.code, now() + Duration::from_secs(11))
			.await
			.expect_err("expired authorization code is evicted");

		let first = store
			.create_at(
				"client",
				"https://app.example/first",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:FIRST")
					.expect("scope parses"),
				user_id.clone(),
				"first",
				now(),
			)
			.await
			.expect("first authorization code creates");
		let second = store
			.create_at(
				"client",
				"https://app.example/second",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:SECOND")
					.expect("scope parses"),
				user_id,
				"second",
				now() + Duration::from_secs(1),
			)
			.await
			.expect("second authorization code creates");

		store
			.consume_at(&first.code, now() + Duration::from_secs(1))
			.await
			.expect_err("oldest authorization code is evicted at capacity");
		store
			.consume_at(&second.code, now() + Duration::from_secs(1))
			.await
			.expect("newest authorization code remains");
	}
}
