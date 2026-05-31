use std::{
	collections::{BTreeMap, VecDeque},
	sync::Arc,
	time::{Duration, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use tokio::sync::Mutex;
use tuwunel_core::{
	Result,
	utils::{hash::sha256, random_string},
};

pub type SessionId = String;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Session {
	pub etag: String,
	pub data: Bytes,
	pub content_type: String,
	pub last_modified: SystemTime,
	pub expires: SystemTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedSession {
	pub id: SessionId,
	pub session: Session,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RendezvousError {
	NotFound,
	ConcurrentWrite,
	TooLarge,
	RateLimited,
}

pub struct Service {
	sessions: Mutex<BTreeMap<SessionId, Session>>,
	rate_limit: Mutex<RateLimit>,
	max_sessions: usize,
	max_bytes: usize,
	ttl: Duration,
}

struct RateLimit {
	max_requests: usize,
	window: Duration,
	requests: VecDeque<SystemTime>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;

		Ok(Self::new_with_rate_limit(
			config.rendezvous_max_sessions,
			config.rendezvous_max_bytes,
			Duration::from_secs(config.rendezvous_ttl_secs),
			config.rendezvous_rate_limit_per_minute,
			Duration::from_secs(60),
		))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	#[must_use]
	pub fn new(max_sessions: usize, max_bytes: usize, ttl: Duration) -> Arc<Self> {
		Self::new_with_rate_limit(
			max_sessions,
			max_bytes,
			ttl,
			usize::MAX,
			Duration::from_secs(60),
		)
	}

	#[must_use]
	pub fn new_with_rate_limit(
		max_sessions: usize,
		max_bytes: usize,
		ttl: Duration,
		max_requests: usize,
		window: Duration,
	) -> Arc<Self> {
		Arc::new(Self {
			sessions: Mutex::new(BTreeMap::new()),
			rate_limit: Mutex::new(RateLimit {
				max_requests: max_requests.max(1),
				window,
				requests: VecDeque::new(),
			}),
			max_sessions: max_sessions.max(1),
			max_bytes,
			ttl,
		})
	}

	#[cfg(test)]
	#[must_use]
	fn new_for_testing(max_sessions: usize, max_bytes: usize, ttl: Duration) -> Arc<Self> {
		Self::new(max_sessions, max_bytes, ttl)
	}

	#[cfg(test)]
	#[must_use]
	fn new_for_testing_with_rate_limit(
		max_sessions: usize,
		max_bytes: usize,
		ttl: Duration,
		max_requests: usize,
		window: Duration,
	) -> Arc<Self> {
		Self::new_with_rate_limit(max_sessions, max_bytes, ttl, max_requests, window)
	}

	pub async fn check_rate_limit(&self) -> Result<(), RendezvousError> {
		self.check_rate_limit_at(SystemTime::now()).await
	}

	pub async fn check_rate_limit_at(&self, now: SystemTime) -> Result<(), RendezvousError> {
		let mut rate_limit = self.rate_limit.lock().await;

		while rate_limit
			.requests
			.front()
			.and_then(|request| request.checked_add(rate_limit.window))
			.is_some_and(|expires| expires <= now)
		{
			rate_limit.requests.pop_front();
		}

		if rate_limit.requests.len() >= rate_limit.max_requests {
			return Err(RendezvousError::RateLimited);
		}

		rate_limit.requests.push_back(now);
		Ok(())
	}

	pub async fn create<S>(
		&self,
		data: Bytes,
		content_type: S,
	) -> Result<CreatedSession, RendezvousError>
	where
		S: Into<String>,
	{
		self.create_at(data, content_type, SystemTime::now())
			.await
	}

	pub async fn create_at<S>(
		&self,
		data: Bytes,
		content_type: S,
		now: SystemTime,
	) -> Result<CreatedSession, RendezvousError>
	where
		S: Into<String>,
	{
		if data.len() > self.max_bytes {
			return Err(RendezvousError::TooLarge);
		}

		let mut sessions = self.sessions.lock().await;
		let id = Self::unused_id(&sessions);
		let session = self.session(data, content_type, now);
		sessions.insert(id.clone(), session.clone());
		self.evict_if_needed(&mut sessions, now);

		Ok(CreatedSession { id, session })
	}

	pub async fn get(&self, id: &str) -> Result<Session, RendezvousError> {
		self.get_at(id, SystemTime::now()).await
	}

	pub async fn get_at(&self, id: &str, now: SystemTime) -> Result<Session, RendezvousError> {
		let mut sessions = self.sessions.lock().await;
		let Some(session) = sessions.get(id) else {
			return Err(RendezvousError::NotFound);
		};

		if Self::is_expired(session, now) {
			sessions.remove(id);
			return Err(RendezvousError::NotFound);
		}

		Ok(session.clone())
	}

	pub async fn update<S>(
		&self,
		id: &str,
		if_match: &str,
		data: Bytes,
		content_type: S,
	) -> Result<Session, RendezvousError>
	where
		S: Into<String>,
	{
		self.update_at(id, if_match, data, content_type, SystemTime::now())
			.await
	}

	pub async fn update_at<S>(
		&self,
		id: &str,
		if_match: &str,
		data: Bytes,
		content_type: S,
		now: SystemTime,
	) -> Result<Session, RendezvousError>
	where
		S: Into<String>,
	{
		if data.len() > self.max_bytes {
			return Err(RendezvousError::TooLarge);
		}

		let mut sessions = self.sessions.lock().await;
		let Some(existing) = sessions.get(id) else {
			return Err(RendezvousError::NotFound);
		};

		if Self::is_expired(existing, now) {
			sessions.remove(id);
			return Err(RendezvousError::NotFound);
		}

		if existing.etag != if_match {
			return Err(RendezvousError::ConcurrentWrite);
		}

		let session = self.session(data, content_type, now);
		sessions.insert(id.to_owned(), session.clone());
		Ok(session)
	}

	pub async fn delete(&self, id: &str) -> Result<(), RendezvousError> {
		self.delete_at(id, SystemTime::now()).await
	}

	pub async fn delete_at(&self, id: &str, now: SystemTime) -> Result<(), RendezvousError> {
		let mut sessions = self.sessions.lock().await;
		let Some(session) = sessions.get(id) else {
			return Err(RendezvousError::NotFound);
		};

		if Self::is_expired(session, now) {
			sessions.remove(id);
			return Err(RendezvousError::NotFound);
		}

		sessions.remove(id);
		Ok(())
	}

	pub async fn len(&self) -> usize { self.sessions.lock().await.len() }

	pub async fn is_empty(&self) -> bool { self.sessions.lock().await.is_empty() }

	fn session(&self, data: Bytes, content_type: impl Into<String>, now: SystemTime) -> Session {
		Session {
			etag: etag(&data),
			data,
			content_type: content_type.into(),
			last_modified: now,
			expires: now.checked_add(self.ttl).unwrap_or(now),
		}
	}

	fn unused_id(sessions: &BTreeMap<SessionId, Session>) -> SessionId {
		loop {
			let id = random_string(16);
			if !sessions.contains_key(&id) {
				return id;
			}
		}
	}

	fn evict_if_needed(&self, sessions: &mut BTreeMap<SessionId, Session>, now: SystemTime) {
		if sessions.len() < self.max_sessions.saturating_mul(2) {
			return;
		}

		sessions.retain(|_, session| !Self::is_expired(session, now));

		while sessions.len() >= self.max_sessions {
			let Some(id) = sessions
				.iter()
				.min_by_key(|(_, session)| session.last_modified)
				.map(|(id, _)| id.clone())
			else {
				break;
			};

			sessions.remove(&id);
		}
	}

	fn is_expired(session: &Session, now: SystemTime) -> bool { now >= session.expires }
}

fn etag(data: &[u8]) -> String { format!("\"{}\"", URL_SAFE_NO_PAD.encode(sha256::hash(data))) }

#[cfg(test)]
mod tests {
	use std::time::{Duration, SystemTime};

	use bytes::Bytes;

	use super::{RendezvousError, Service};

	const TEXT_PLAIN: &str = "text/plain";

	#[tokio::test]
	async fn rendezvous_create_get_update_delete_enforces_etags_and_ttl() {
		let service = Service::new_for_testing(10, 4, Duration::from_secs(60));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

		let created = service
			.create_at(Bytes::from_static(b"one"), TEXT_PLAIN, now)
			.await
			.expect("create succeeds");

		assert_eq!(created.session.data, Bytes::from_static(b"one"));
		assert_eq!(created.session.content_type, TEXT_PLAIN);
		assert_eq!(created.session.last_modified, now);
		assert_eq!(created.session.expires, after(now, 60));
		assert!(created.session.etag.starts_with('"'));
		assert!(created.session.etag.ends_with('"'));

		let fetched = service
			.get_at(&created.id, after(now, 1))
			.await
			.expect("get before expiry succeeds");
		assert_eq!(fetched.etag, created.session.etag);
		assert_eq!(fetched.data, Bytes::from_static(b"one"));

		let stale = service
			.update_at(
				&created.id,
				"\"stale\"",
				Bytes::from_static(b"two"),
				TEXT_PLAIN,
				after(now, 2),
			)
			.await;
		assert!(matches!(stale, Err(RendezvousError::ConcurrentWrite)));

		let updated = service
			.update_at(
				&created.id,
				&created.session.etag,
				Bytes::from_static(b"two"),
				TEXT_PLAIN,
				after(now, 3),
			)
			.await
			.expect("put with current etag succeeds");
		assert_ne!(updated.etag, created.session.etag);
		assert_eq!(updated.data, Bytes::from_static(b"two"));
		assert_eq!(updated.last_modified, after(now, 3));
		assert_eq!(updated.expires, after(now, 63));

		let expired = service.get_at(&created.id, after(now, 64)).await;
		assert!(matches!(expired, Err(RendezvousError::NotFound)));

		let second = service
			.create_at(Bytes::from_static(b"bye"), TEXT_PLAIN, now)
			.await
			.expect("second create succeeds");
		service
			.delete_at(&second.id, after(now, 1))
			.await
			.expect("delete succeeds");
		assert!(matches!(
			service.get_at(&second.id, after(now, 2)).await,
			Err(RendezvousError::NotFound),
		));
	}

	#[tokio::test]
	async fn rendezvous_rejects_payloads_over_the_configured_limit() {
		let service = Service::new_for_testing(10, 4, Duration::from_secs(60));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

		let create = service
			.create_at(Bytes::from_static(b"12345"), TEXT_PLAIN, now)
			.await;
		assert!(matches!(create, Err(RendezvousError::TooLarge)));

		let created = service
			.create_at(Bytes::from_static(b"1234"), TEXT_PLAIN, now)
			.await
			.expect("max-sized payload is accepted");

		let update = service
			.update_at(
				&created.id,
				&created.session.etag,
				Bytes::from_static(b"12345"),
				TEXT_PLAIN,
				after(now, 1),
			)
			.await;
		assert!(matches!(update, Err(RendezvousError::TooLarge)));
	}

	#[tokio::test]
	async fn rendezvous_evicts_expired_sessions_before_oldest_sessions() {
		let service = Service::new_for_testing(2, 32, Duration::from_secs(10));
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

		let expired = service
			.create_at(Bytes::from_static(b"expired"), TEXT_PLAIN, now)
			.await
			.expect("create succeeds");
		let oldest = service
			.create_at(Bytes::from_static(b"oldest"), TEXT_PLAIN, after(now, 11))
			.await
			.expect("create succeeds");
		let survivor = service
			.create_at(Bytes::from_static(b"survivor"), TEXT_PLAIN, after(now, 12))
			.await
			.expect("create succeeds");
		let newest = service
			.create_at(Bytes::from_static(b"newest"), TEXT_PLAIN, after(now, 13))
			.await
			.expect("create succeeds");

		assert!(matches!(
			service.get_at(&expired.id, after(now, 13)).await,
			Err(RendezvousError::NotFound),
		));
		assert!(matches!(
			service.get_at(&oldest.id, after(now, 13)).await,
			Err(RendezvousError::NotFound),
		));
		assert!(matches!(
			service.get_at(&survivor.id, after(now, 13)).await,
			Err(RendezvousError::NotFound),
		));
		service
			.get_at(&newest.id, after(now, 13))
			.await
			.expect("newest session survives eviction");
		assert_eq!(service.len().await, 1);
	}

	#[tokio::test]
	async fn rendezvous_rate_limit_rejects_requests_over_the_window_limit() {
		let service = Service::new_for_testing_with_rate_limit(
			10,
			32,
			Duration::from_secs(10),
			2,
			Duration::from_secs(60),
		);
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);

		service
			.check_rate_limit_at(now)
			.await
			.expect("first request accepted");
		service
			.check_rate_limit_at(after(now, 1))
			.await
			.expect("second request accepted");
		assert!(matches!(
			service.check_rate_limit_at(after(now, 2)).await,
			Err(RendezvousError::RateLimited),
		));
		service
			.check_rate_limit_at(after(now, 61))
			.await
			.expect("request accepted after the window expires");
	}

	fn after(now: SystemTime, seconds: u64) -> SystemTime {
		now.checked_add(Duration::from_secs(seconds))
			.expect("test timestamp does not overflow")
	}
}
