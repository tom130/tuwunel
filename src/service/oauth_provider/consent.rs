use std::{
	collections::HashMap,
	sync::Arc,
	time::{Duration, SystemTime},
};

use ruma::OwnedUserId;
use tokio::sync::Mutex;
use tuwunel_core::{Err, Result, err, utils::random_string};

const SESSION_ID_LENGTH: usize = 32;
const CSRF_TOKEN_LENGTH: usize = 32;

pub struct Consent {
	state: Mutex<State>,
	ttl: Duration,
}

#[derive(Default)]
struct State {
	sessions: HashMap<String, BrowserSession>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedSession {
	pub session_id: String,
	pub session: BrowserSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserSession {
	pub user_id: OwnedUserId,
	pub csrf_token: String,
	pub expires: SystemTime,
}

impl Consent {
	pub(super) fn build(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(State::default()),
			ttl: Duration::from_secs(args.server.config.consent_session_ttl_secs),
		})
	}

	#[cfg(test)]
	pub(crate) fn new_for_testing(ttl: Duration) -> Arc<Self> {
		Arc::new(Self { state: Mutex::new(State::default()), ttl })
	}

	pub async fn create(&self, user_id: OwnedUserId) -> CreatedSession {
		self.create_at(user_id, SystemTime::now()).await
	}

	pub async fn create_at(&self, user_id: OwnedUserId, now: SystemTime) -> CreatedSession {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);

		let session_id = unused_session_id(&state);
		let session = BrowserSession {
			user_id,
			csrf_token: random_string(CSRF_TOKEN_LENGTH),
			expires: now.checked_add(self.ttl).unwrap_or(now),
		};

		state
			.sessions
			.insert(session_id.clone(), session.clone());

		CreatedSession { session_id, session }
	}

	pub async fn get(&self, session_id: &str) -> Result<BrowserSession> {
		self.get_at(session_id, SystemTime::now()).await
	}

	pub async fn get_at(&self, session_id: &str, now: SystemTime) -> Result<BrowserSession> {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		state
			.sessions
			.get(session_id)
			.cloned()
			.ok_or_else(session_not_found)
	}

	pub async fn validate(&self, session_id: &str, csrf_token: &str) -> Result<BrowserSession> {
		self.validate_at(session_id, csrf_token, SystemTime::now())
			.await
	}

	pub async fn validate_at(
		&self,
		session_id: &str,
		csrf_token: &str,
		now: SystemTime,
	) -> Result<BrowserSession> {
		let session = self.get_at(session_id, now).await?;
		if session.csrf_token != csrf_token {
			return Err!(Request(Forbidden("Invalid CSRF token.")));
		}

		Ok(session)
	}

	fn evict_expired(state: &mut State, now: SystemTime) {
		state
			.sessions
			.retain(|_, session| now < session.expires);
	}
}

fn unused_session_id(state: &State) -> String {
	loop {
		let session_id = random_string(SESSION_ID_LENGTH);
		if !state.sessions.contains_key(&session_id) {
			return session_id;
		}
	}
}

fn session_not_found() -> tuwunel_core::Error {
	err!(Request(Unauthorized("Consent session is missing or expired.")))
}

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use ruma::owned_user_id;

	use super::Consent;

	#[tokio::test]
	async fn oauth_consent_session_validates_csrf_and_expires() {
		let consent = Consent::new_for_testing(Duration::from_secs(60));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let created = consent
			.create_at(owned_user_id!("@alice:example.com"), now)
			.await;

		let session = consent
			.validate_at(&created.session_id, &created.session.csrf_token, now)
			.await
			.expect("csrf validates");
		assert_eq!(session.user_id, owned_user_id!("@alice:example.com"));

		assert!(
			consent
				.validate_at(&created.session_id, "wrong-csrf", now)
				.await
				.is_err(),
			"wrong csrf is rejected",
		);
		assert!(
			consent
				.get_at(&created.session_id, now + Duration::from_secs(61))
				.await
				.is_err(),
			"expired session is removed",
		);
	}
}
