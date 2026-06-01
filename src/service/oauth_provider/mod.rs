pub mod authorize;
pub mod clients;
pub mod consent;
pub mod grants;
pub mod pkce;
pub mod redirect;
pub mod scope;

use std::sync::Arc;

use tuwunel_core::Result;

pub struct Service {
	pub authorize: Arc<authorize::Authorizations>,
	pub clients: Arc<clients::Clients>,
	pub consent: Arc<consent::Consent>,
	pub grants: Arc<grants::Grants>,
}

impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			authorize: authorize::Authorizations::build(args),
			clients: Arc::new(clients::Clients::build(args)),
			consent: consent::Consent::build(args),
			grants: grants::Grants::build(args),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}
