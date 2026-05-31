use std::{
	collections::HashMap,
	sync::Arc,
	time::{Duration, SystemTime},
};

use ruma::OwnedUserId;
use tokio::sync::Mutex;
use tuwunel_core::{Result, utils::random_string};

use super::scope::GrantedScope;

const DEVICE_CODE_LENGTH: usize = 32;
const USER_CODE_LENGTH: usize = 8;

pub struct Grants {
	state: Mutex<State>,
	expires_after: Duration,
	interval: Duration,
}

#[derive(Default)]
struct State {
	grants: HashMap<String, PendingGrant>,
	user_codes: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedGrant {
	pub device_code: String,
	pub user_code: String,
	pub grant: PendingGrant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingGrant {
	pub client_id: String,
	pub granted_scope: GrantedScope,
	pub device_id: String,
	pub user_code: String,
	pub created: SystemTime,
	pub expires: SystemTime,
	pub interval: Duration,
	pub last_polled: Option<SystemTime>,
	pub status: GrantStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantStatus {
	Pending,
	Approved {
		user_id: OwnedUserId,
	},
	Denied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantPoll {
	Ready(Box<PendingGrant>),
	SlowDown {
		interval: Duration,
	},
	Expired,
}

impl Grants {
	pub(super) fn build(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(State::default()),
			expires_after: Duration::from_secs(args.server.config.device_grant_expires_secs),
			interval: Duration::from_secs(args.server.config.device_grant_interval_secs),
		})
	}

	#[cfg(test)]
	fn new_for_testing(expires_after: Duration, interval: Duration) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(State::default()),
			expires_after,
			interval,
		})
	}

	pub async fn create<S>(
		&self,
		client_id: S,
		granted_scope: GrantedScope,
	) -> Result<CreatedGrant>
	where
		S: Into<String>,
	{
		self.create_at(client_id, granted_scope, SystemTime::now())
			.await
	}

	pub async fn create_at<S>(
		&self,
		client_id: S,
		granted_scope: GrantedScope,
		now: SystemTime,
	) -> Result<CreatedGrant>
	where
		S: Into<String>,
	{
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);

		let device_code = unused_device_code(&state);
		let user_code = unused_user_code(&state);
		let grant = PendingGrant {
			client_id: client_id.into(),
			device_id: granted_scope.device_id.clone(),
			granted_scope,
			user_code: user_code.clone(),
			created: now,
			expires: now.checked_add(self.expires_after).unwrap_or(now),
			interval: self.interval,
			last_polled: None,
			status: GrantStatus::Pending,
		};

		state
			.user_codes
			.insert(user_code.clone(), device_code.clone());
		state
			.grants
			.insert(device_code.clone(), grant.clone());

		Ok(CreatedGrant { device_code, user_code, grant })
	}

	pub async fn by_device_code(&self, device_code: &str) -> Result<PendingGrant> {
		self.by_device_code_at(device_code, SystemTime::now())
			.await
	}

	pub async fn by_device_code_at(
		&self,
		device_code: &str,
		now: SystemTime,
	) -> Result<PendingGrant> {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		state
			.grants
			.get(device_code)
			.cloned()
			.ok_or_else(not_found)
	}

	pub async fn poll(&self, device_code: &str) -> Result<GrantPoll> {
		self.poll_at(device_code, SystemTime::now()).await
	}

	pub async fn poll_at(&self, device_code: &str, now: SystemTime) -> Result<GrantPoll> {
		let mut state = self.state.lock().await;

		if state
			.grants
			.get(device_code)
			.is_some_and(|grant| now >= grant.expires)
		{
			remove_device_code(&mut state, device_code);
			return Ok(GrantPoll::Expired);
		}

		Self::evict_expired(&mut state, now);
		let grant = state
			.grants
			.get_mut(device_code)
			.ok_or_else(not_found)?;

		if grant
			.last_polled
			.is_some_and(|last_polled| polled_too_soon(last_polled, now, grant.interval))
		{
			grant.interval = grant
				.interval
				.checked_add(self.interval)
				.unwrap_or(grant.interval);
			grant.last_polled = Some(now);

			return Ok(GrantPoll::SlowDown { interval: grant.interval });
		}

		grant.last_polled = Some(now);
		Ok(GrantPoll::Ready(Box::new(grant.clone())))
	}

	pub async fn by_user_code(&self, user_code: &str) -> Result<PendingGrant> {
		self.by_user_code_at(user_code, SystemTime::now())
			.await
	}

	pub async fn by_user_code_at(
		&self,
		user_code: &str,
		now: SystemTime,
	) -> Result<PendingGrant> {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		let device_code = state
			.user_codes
			.get(user_code)
			.ok_or_else(not_found)?;

		state
			.grants
			.get(device_code)
			.cloned()
			.ok_or_else(not_found)
	}

	pub async fn approve_at(
		&self,
		user_code: &str,
		user_id: OwnedUserId,
		now: SystemTime,
	) -> Result {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		let device_code = state
			.user_codes
			.get(user_code)
			.cloned()
			.ok_or_else(not_found)?;
		let grant = state
			.grants
			.get_mut(&device_code)
			.ok_or_else(not_found)?;

		grant.status = GrantStatus::Approved { user_id };
		Ok(())
	}

	pub async fn approve(&self, user_code: &str, user_id: OwnedUserId) -> Result {
		self.approve_at(user_code, user_id, SystemTime::now())
			.await
	}

	pub async fn deny_at(&self, user_code: &str, now: SystemTime) -> Result {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		let device_code = state
			.user_codes
			.get(user_code)
			.cloned()
			.ok_or_else(not_found)?;
		let grant = state
			.grants
			.get_mut(&device_code)
			.ok_or_else(not_found)?;

		grant.status = GrantStatus::Denied;
		Ok(())
	}

	pub async fn deny(&self, user_code: &str) -> Result {
		self.deny_at(user_code, SystemTime::now()).await
	}

	pub async fn consume_at(&self, device_code: &str, now: SystemTime) -> Result<PendingGrant> {
		let mut state = self.state.lock().await;
		Self::evict_expired(&mut state, now);
		remove_device_code(&mut state, device_code).ok_or_else(not_found)
	}

	pub async fn consume(&self, device_code: &str) -> Result<PendingGrant> {
		self.consume_at(device_code, SystemTime::now())
			.await
	}

	fn evict_expired(state: &mut State, now: SystemTime) {
		let expired: Vec<_> = state
			.grants
			.iter()
			.filter(|(_, grant)| now >= grant.expires)
			.map(|(device_code, _)| device_code.clone())
			.collect();

		for device_code in expired {
			remove_device_code(state, &device_code);
		}
	}
}

fn unused_device_code(state: &State) -> String {
	loop {
		let code = random_string(DEVICE_CODE_LENGTH);
		if !state.grants.contains_key(&code) {
			return code;
		}
	}
}

fn unused_user_code(state: &State) -> String {
	loop {
		let code = random_string(USER_CODE_LENGTH).to_ascii_uppercase();
		let mut chars = code.chars();
		let first = chars.by_ref().take(4).collect::<String>();
		let second = chars.collect::<String>();
		let code = format!("{first}-{second}");
		if !state.user_codes.contains_key(&code) {
			return code;
		}
	}
}

fn remove_device_code(state: &mut State, device_code: &str) -> Option<PendingGrant> {
	let grant = state.grants.remove(device_code)?;
	state.user_codes.remove(&grant.user_code);
	Some(grant)
}

fn polled_too_soon(last_polled: SystemTime, now: SystemTime, interval: Duration) -> bool {
	now.duration_since(last_polled)
		.map_or(true, |elapsed| elapsed < interval)
}

fn not_found() -> tuwunel_core::Error {
	tuwunel_core::err!(Request(NotFound("Device grant not found.")))
}

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use ruma::owned_user_id;

	use super::{GrantPoll, GrantStatus, Grants};
	use crate::oauth_provider::scope::parse_scope;

	#[tokio::test]
	async fn device_grant_create_lookup_approve_deny_and_consume() {
		let grants = Grants::new_for_testing(Duration::from_secs(600), Duration::from_secs(5));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let scope = parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
			.expect("scope parses");

		let created = grants
			.create_at("client", scope, now)
			.await
			.expect("grant creates");

		assert_eq!(created.grant.client_id, "client");
		assert_eq!(created.grant.device_id, "DEVICE");
		assert_eq!(created.grant.interval, Duration::from_secs(5));
		assert_eq!(created.grant.expires, now + Duration::from_secs(600));

		let by_device = grants
			.by_device_code_at(&created.device_code, now)
			.await
			.expect("device_code lookup works");
		assert_eq!(by_device.user_code, created.user_code);

		grants
			.approve_at(&created.user_code, owned_user_id!("@user:example.com"), now)
			.await
			.expect("approve works");
		assert!(matches!(
			grants
				.by_device_code_at(&created.device_code, now)
				.await
				.expect("approved grant is readable")
				.status,
			GrantStatus::Approved { .. },
		));

		let consumed = grants
			.consume_at(&created.device_code, now)
			.await
			.expect("consume works");
		assert_eq!(consumed.user_code, created.user_code);
		grants
			.by_device_code_at(&created.device_code, now)
			.await
			.unwrap_err();

		let denied = grants
			.create_at(
				"client",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DENIED")
					.expect("scope parses"),
				now,
			)
			.await
			.expect("grant creates");
		grants
			.deny_at(&denied.user_code, now)
			.await
			.expect("deny works");
		assert!(matches!(
			grants
				.by_device_code_at(&denied.device_code, now)
				.await
				.expect("denied grant is readable")
				.status,
			GrantStatus::Denied,
		));
	}

	#[tokio::test]
	async fn device_grant_expires_and_removes_user_code_index() {
		let grants = Grants::new_for_testing(Duration::from_secs(10), Duration::from_secs(5));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let created = grants
			.create_at(
				"client",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
					.expect("scope parses"),
				now,
			)
			.await
			.expect("grant creates");

		grants
			.by_user_code_at(&created.user_code, now + Duration::from_secs(11))
			.await
			.unwrap_err();
		grants
			.by_device_code_at(&created.device_code, now + Duration::from_secs(11))
			.await
			.unwrap_err();
	}

	#[tokio::test]
	async fn device_grant_poll_tracks_pending_slow_down_and_expired() {
		let grants = Grants::new_for_testing(Duration::from_secs(10), Duration::from_secs(5));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let created = grants
			.create_at(
				"client",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
					.expect("scope parses"),
				now,
			)
			.await
			.expect("grant creates");

		let GrantPoll::Ready(first_poll) = grants
			.poll_at(&created.device_code, now)
			.await
			.expect("first poll succeeds")
		else {
			panic!("first poll should read the pending grant");
		};
		assert!(matches!(first_poll.status, GrantStatus::Pending));

		let GrantPoll::SlowDown { interval } = grants
			.poll_at(&created.device_code, now + Duration::from_secs(1))
			.await
			.expect("fast poll succeeds as slow_down")
		else {
			panic!("fast poll should slow down");
		};
		assert!(interval > Duration::from_secs(5));

		let GrantPoll::Expired = grants
			.poll_at(&created.device_code, now + Duration::from_secs(11))
			.await
			.expect("expired poll succeeds as expired")
		else {
			panic!("expired grant should return expired");
		};
		grants
			.by_device_code_at(&created.device_code, now)
			.await
			.unwrap_err();
	}
}
