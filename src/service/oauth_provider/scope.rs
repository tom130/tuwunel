use std::fmt;

use tuwunel_core::{Err, Result};

const STABLE_API_SCOPE: &str = "urn:matrix:client:api:*";
const UNSTABLE_API_SCOPE: &str = "urn:matrix:org.matrix.msc2967.client:api:*";
const STABLE_DEVICE_SCOPE_PREFIX: &str = "urn:matrix:client:device:";
const UNSTABLE_DEVICE_SCOPE_PREFIX: &str = "urn:matrix:org.matrix.msc2967.client:device:";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrantedScope {
	pub device_id: String,
	device_scope: String,
	api_scope: String,
}

pub fn parse_scope(scope: &str) -> Result<GrantedScope> {
	let mut api_scope = None;
	let mut device_scope = None;

	for token in scope.split_whitespace() {
		match token {
			| STABLE_API_SCOPE | UNSTABLE_API_SCOPE if api_scope.is_none() => {
				api_scope = Some(token.to_owned());
			},
			| STABLE_API_SCOPE | UNSTABLE_API_SCOPE => {},
			| _ => {
				let device_id = token
					.strip_prefix(STABLE_DEVICE_SCOPE_PREFIX)
					.or_else(|| token.strip_prefix(UNSTABLE_DEVICE_SCOPE_PREFIX));

				if let Some(device_id) = device_id {
					if device_scope.is_some() {
						return Err!(Request(InvalidParam(
							"scope contains duplicate device scopes.",
						)));
					}

					validate_device_id(device_id)?;
					device_scope = Some((token.to_owned(), device_id.to_owned()));
				}
			},
		}
	}

	let (device_scope, device_id) = device_scope.ok_or_else(|| {
		tuwunel_core::err!(Request(InvalidParam("scope is missing a device scope.")))
	})?;
	let api_scope = api_scope.ok_or_else(|| {
		tuwunel_core::err!(Request(InvalidParam("scope is missing the Matrix API scope.")))
	})?;

	Ok(GrantedScope { device_id, device_scope, api_scope })
}

impl fmt::Display for GrantedScope {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{} {}", self.device_scope, self.api_scope)
	}
}

fn validate_device_id(device_id: &str) -> Result {
	if device_id.is_empty() {
		return Err!(Request(InvalidParam("device scope has an empty device id.")));
	}

	if !device_id.bytes().all(|byte| {
		matches!(
			byte,
			b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'~' | b'-'
		)
	}) {
		return Err!(Request(InvalidParam("device scope has invalid device id characters.")));
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::parse_scope;

	#[test]
	fn oauth_scope_parses_stable_device_and_api_scopes() {
		let granted =
			parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE.1_2-3~4")
				.expect("stable scope parses");

		assert_eq!(granted.device_id, "DEVICE.1_2-3~4");
		assert_eq!(
			granted.to_string(),
			"urn:matrix:client:device:DEVICE.1_2-3~4 urn:matrix:client:api:*",
		);
	}

	#[test]
	fn oauth_scope_parses_unstable_device_and_api_scopes() {
		let granted = parse_scope(
			"urn:matrix:org.matrix.msc2967.client:api:* \
			 urn:matrix:org.matrix.msc2967.client:device:CURVE25519",
		)
		.expect("unstable scope parses");

		assert_eq!(granted.device_id, "CURVE25519");
		assert_eq!(
			granted.to_string(),
			"urn:matrix:org.matrix.msc2967.client:device:CURVE25519 \
			 urn:matrix:org.matrix.msc2967.client:api:*",
		);
	}

	#[test]
	fn oauth_scope_rejects_missing_duplicate_or_invalid_device_scope() {
		parse_scope("urn:matrix:client:api:*").unwrap_err();
		parse_scope(
			"urn:matrix:client:api:* urn:matrix:client:device:A \
			 urn:matrix:org.matrix.msc2967.client:device:B",
		)
		.unwrap_err();
		parse_scope("urn:matrix:client:api:* urn:matrix:client:device:bad/id").unwrap_err();
	}

	#[test]
	fn oauth_scope_rejects_missing_api_scope() {
		parse_scope("urn:matrix:client:device:DEVICE").unwrap_err();
	}
}
